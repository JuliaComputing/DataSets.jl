# Many datasets have tree-like indices.  Examples:
#
#        Index        Data
#
# * OS:  directories           files
# * Git: trees                 blobs
# * S3:  prefixes              blobs
# * HDF5 group                 typed data
# * Zip  flattend directory(?) blobs
#

import AbstractTrees: AbstractTrees, children

#-------------------------------------------------------------------------------
abstract type AbstractBlobTree; end

# The tree API

# TODO: Should we have `istree` separate from `isdir`?
Base.isdir(x::AbstractBlobTree) = true
Base.isfile(tree::AbstractBlobTree) = false
Base.ispath(x::AbstractBlobTree) = true

# Number of children is not known without a (potentially high-latency) call to
# an external resource
Base.IteratorSize(tree::AbstractBlobTree) = Base.SizeUnknown()

function Base.iterate(tree::AbstractBlobTree, state=nothing)
    if state == nothing
        # By default, call `children(tree)` to eagerly get a list of children
        # for iteration.
        cs = children(tree)
        itr = iterate(cs)
    else
        (cs, cstate) = state
        itr = iterate(cs, cstate)
    end
    if itr == nothing
        return nothing
    else
        (c, cstate) = itr
        (c, (cs, cstate))
    end
end

"""
    children(tree::AbstractBlobTree)

Return an array of the children of `tree`. A child `x` may abstractly either be
another tree (`children(x)` returns a collection) or a file, where `children(x)`
returns `()`.

Note that this is subtly different from `readdir(path)` which returns relative
paths, or `readdir(path, join=true)` which returns absolute paths.
"""
function children(tree::AbstractBlobTree)
    # TODO: Is dispatch to the root a correct default?
    children(tree.root, tree.path)
end


"""
    showtree([io,], tree)

Pretty printing of file trees, in the spirit of the unix `tree` utility.
"""
function showtree(io::IO, tree::AbstractBlobTree; maxdepth=5)
    println(io, "📂 ", tree)
    _showtree(io, tree, "", maxdepth)
end

struct ShownTree
    tree
end
# Use a wrapper rather than defaulting to stdout so that this works in more
# functional environments such as Pluto.jl
showtree(tree::AbstractBlobTree) = ShownTree(tree)

Base.show(io::IO, s::ShownTree) = showtree(io, s.tree)

function _showtree(io::IO, tree::AbstractBlobTree, prefix, depth)
    cs = children(tree)
    for (i,x) in enumerate(cs)
        islast = i == lastindex(cs) # TODO: won't work if children() is lazy
        first_prefix = prefix * (islast ? "└──" : "├──")
        other_prefix = prefix * (islast ? "   " : "│  ")
        if isdir(x)
            print(io, first_prefix, "📂 ")
            printstyled(io, basename(x), "\n", color=:light_blue, bold=true)
            if depth > 1
                _showtree(io, x, other_prefix, depth-1)
            else
                print(io, other_prefix, '⋮')
            end
        else
            println(io, first_prefix, " ", basename(x))
        end
    end
end

function Base.copy!(dst::AbstractBlobTree, src::AbstractBlobTree)
    for x in src
        newpath = joinpath(dst, basename(x))
        if isdir(x)
            newdir = mkdir(newpath)
            copy!(newdir, x)
        else
            open(x) do io_src
                open(newpath, write=true) do io_dst
                    write(io_dst, io_src)
                end
            end
        end
    end
end

#-------------------------------------------------------------------------------
"""
    Blob(root)
    Blob(root, relpath)

`Blob` represents the location of a collection of unstructured binary data. The
location is a path `relpath` relative to some `root` data resource.

A `Blob` can naturally be `open()`ed as a `Vector{UInt8}`, but can also be
mapped into the program as an `IO` byte stream, or interpreted as a `String`.

Blobs can be arranged into hierarchies "directories" via the `BlobTree` type.
"""
struct Blob{Root}
    root::Root
    path::RelPath
end

Blob(root) = Blob(root, RelPath())

_root_resource(b::Blob) = b.root

Base.basename(file::Blob) = basename(file.path)
Base.abspath(file::Blob) = AbsPath(file.root, file.path)
Base.isdir(file::Blob) = false
Base.isfile(file::Blob) = true
Base.ispath(file::Blob) = true

function Base.show(io::IO, ::MIME"text/plain", file::Blob)
    print(io, "📄 ", file.path, " @ ", summary(file.root))
end

function AbstractTrees.printnode(io::IO, file::Blob)
    print(io, "📄 ",  basename(file))
end

# Opening as Vector{UInt8} or as String uses IO interface
function Base.open(f::Function, ::Type{Vector{UInt8}}, file::Blob)
    open(IO, file.root, file.path) do io
        f(read(io)) # TODO: use Mmap?
    end
end

function Base.open(f::Function, ::Type{String}, file::Blob)
    open(IO, file.root, file.path) do io
        f(read(io, String))
    end
end

# Default open-type for Blob is IO
Base.open(f::Function, file::Blob; kws...) = open(f, IO, file.root, file.path; kws...)

# Opening Blob as itself is trivial
function Base.open(f::Function, ::Type{Blob}, file::Blob)
    f(file)
end

# open with other types T defers to the underlying storage system
function Base.open(f::Function, ::Type{T}, file::Blob; kws...) where {T}
    open(f, T, file.root, file.path; kws...)
end

# Unscoped form of open for Blob
function Base.open(::Type{T}, file::Blob; kws...) where {T}
    call_with_finalized_context() do ctx
        open(ctx, T, file; kws...)
    end
end

# Contexts.jl - based versions of the above.

@! function Base.open(::Type{Vector{UInt8}}, file::Blob)
    @context begin
        # TODO: use Mmap?
        read(@! open(IO, file.root, file.path))
    end
end

@! function Base.open(::Type{String}, file::Blob)
    @context begin
        read(@!(open(IO, file.root, file.path)), String)
    end
end

# Default open-type for Blob is IO
@! function Base.open(file::Blob; kws...)
    @! open(IO, file.root, file.path; kws...)
end

# Opening Blob as itself is trivial
@! function Base.open(::Type{Blob}, file::Blob)
    file
end

# open with other types T defers to the underlying storage system
@! function Base.open(::Type{T}, file::Blob; kws...) where {T}
    @! open(T, file.root, file.path; kws...)
end

# read() is also supported for `Blob`s
Base.read(file::Blob) = read(file.root, file.path)
Base.read(file::Blob, ::Type{T}) where {T} = read(file.root, file.path, T)


# Support for opening AbsPath
#
# TODO: Put this elsewhere?
function Base.open(f::Function, ::Type{T}, path::AbsPath; kws...) where {T}
    open(f, T, path.root, path.path; kws...)
end

Base.open(f::Function, path::AbsPath; kws...) = open(f, IO, path.root, path.path; kws...)


#-------------------------------------------------------------------------------
"""
    BlobTree(root)

`BlobTree` is a "directory tree" like hierarchy which may have `Blob`s and
`BlobTree`s as children.

The tree implements the `AbstracTrees.children()` interface and may be indexed
with paths to traverse the hierarchy down to the leaves ("files") which are of
type `Blob`. Individual leaves may be `open()`ed as various Julia types.

# Example

Normally you'd construct these via the [`dataset`](@ref) function which takes
care of constructing the correct `root` object. However, here's a direct
demonstration:

```
julia> tree = BlobTree(DataSets.FileSystemRoot(dirname(pathof(DataSets))), path"../test/data")
📂 Tree ../test/data @ /home/chris/.julia/dev/DataSets/src
 📁 csvset
 📄 file.txt
 📄 foo.txt
 📄 people.csv.gz

julia> tree["csvset"]
📂 Tree ../test/data/csvset @ /home/chris/.julia/dev/DataSets/src
 📄 1.csv
 📄 2.csv

julia> tree[path"csvset"]
📂 Tree ../test/data/csvset @ /home/chris/.julia/dev/DataSets/src
 📄 1.csv
 📄 2.csv
```
"""
struct BlobTree{Root} <: AbstractBlobTree
    root::Root
    path::RelPath
end

BlobTree(root) = BlobTree(root, RelPath())

_root_resource(b::BlobTree) = b.root

function AbstractTrees.printnode(io::IO, tree::BlobTree)
    print(io, "📂 ",  basename(tree))
end

function Base.show(io::IO, ::MIME"text/plain", tree::AbstractBlobTree)
    # TODO: Ideally we'd use
    # AbstractTrees.print_tree(io, tree, 1)
    # However, this is hard to use efficiently; we'd need to implement a lazy
    # `children()` for all our trees. It'd be much easier if
    # `AbstractTrees.has_children()` was used consistently upstream.
    cs = children(tree)
    println(io, "📂 Tree ", tree.path, " @ ", summary(tree.root))
    for (i, c) in enumerate(cs)
        print(io, " ", isdir(c) ? '📁' : '📄', " ", basename(c))
        if i != length(cs)
            print(io, '\n')
        end
    end
end

Base.basename(tree::BlobTree) = basename(tree.path)
Base.abspath(tree::BlobTree) = AbsPath(tree.root, tree.path)

# getindex vs joinpath:
#  - getindex about indexing the datastrcutre; therefore it looks in the
#    filesystem to only return things which exist.
#  - joinpath just makes paths, not knowing whether they exist.
function Base.getindex(tree::BlobTree, path::RelPath)
    relpath = joinpath(tree.path, path)
    root = tree.root
    if isdir(root, relpath)
        BlobTree(root, relpath)
    elseif isfile(root, relpath)
        Blob(root, relpath)
    elseif ispath(root, relpath)
        AbsPath(root, relpath) # Not great?
    else
        error("Path $relpath @ $root doesn't exist")
    end
end

function Base.getindex(tree::BlobTree, name::AbstractString)
    getindex(tree, joinpath(RelPath(), name))
end

# We've got a weird mishmash of path vs tree handling here.
# TODO: Can we refactor this to cleanly separate the filesystem commands (which
# take abstract paths?) from BlobTree and Blob which act as an abstraction over
# the filesystem or other storage mechanisms?
function Base.joinpath(tree::BlobTree, r::RelPath)
    AbsPath(tree.root, joinpath(tree.path, r))
end

function Base.joinpath(tree::BlobTree, s::AbstractString)
    AbsPath(tree.root, joinpath(tree.path, s))
end

function Base.haskey(tree::BlobTree, name::AbstractString)
    ispath(tree.root, joinpath(tree.path, name))
end

function Base.readdir(tree::BlobTree)
    readdir(tree.root, tree.path)
end

function Base.rm(tree::BlobTree; kws...)
    rm(tree.root, tree.path; kws...)
end

function children(tree::BlobTree)
    child_names = readdir(tree)
    [tree[c] for c in child_names]
end

function Base.open(f::Function, ::Type{BlobTree}, tree::BlobTree)
    f(tree)
end

@! function Base.open(::Type{BlobTree}, tree::BlobTree)
    tree
end

# Base.open(::Type{T}, file::Blob; kws...) where {T} = open(identity, T, file.root, file.path; kws...)
