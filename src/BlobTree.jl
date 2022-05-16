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
        xname = basename(x)
        if isdir(x)
            copy!(newdir(dst, xname), x)
        else
            open(x) do io_src
                newfile(dst, xname, write=true) do io_dst
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
mutable struct Blob{Root}
    root::Root
    path::RelPath
end

Blob(root) = Blob(root, RelPath())

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

# Opening as Vector{UInt8} or as String defers to IO interface
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

# ResourceContexts.jl - based versions of the above.

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

# Fallback implementation of `@! open(T, root, path)` based on enter_do.
#
# TODO: Update other backends to avoid calling this; using enter_do is pretty
# inefficient.
@! function Base.open(::Type{T}, root, path; kws...) where {T}
    (res,) = @! enter_do(open, T, root, path; kws...)
    res
end

# Unscoped form of open for Blob
function Base.open(::Type{T}, blob::Blob; kws...) where {T}
    @context begin
        result = @! open(T, blob; kws...)
        @! ResourceContexts.detach_context_cleanup(result)
    end
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

# Operations on BlobTree

BlobTree has a largely dictionary-like interface:

* List keys (ie, file and directory names): `keys(tree)`
* List keys and values:  `pairs(tree)`
* Query keys:            `haskey(tree)`
* Traverse the tree:     `tree["path"]`
* Add new content:       `newdir(tree, "path")`, `newfile(tree, "path")`
* Delete content:        `delete!(tree, "path")`

Unlike Dict, iteration of BlobTree iterates values (not key value pairs). This
has some benefits - for example, broadcasting processing across files in a
directory.

* Property access
  - `isdir()`, `isfile()` - determine whether a child of tree is a directory or file.

# Example

You can create a new temporary BlobTree via the `newdir()` function:

```
julia> dir = newdir()
       for i = 1:3
           newfile(dir, "\$i/a.txt") do io
               println(io, "Content of a")
           end
           newfile(dir, "b-\$i.txt") do io
               println(io, "Content of b")
           end
       end
       dir
📂 Tree  @ /tmp/jl_Sp6wMF
 📁 1
 📁 2
 📁 3
 📄 b-1.txt
 📄 b-2.txt
 📄 b-3.txt
```

You can also get access to a `BlobTree` by using `DataSets.from_path()` with a
local directory name. For example:

```
julia> using Pkg
       open(DataSets.from_path(joinpath(Pkg.dir("DataSets"), "src")))
📂 Tree  @ ~/.julia/dev/DataSets/src
 📄 DataSet.jl
 📄 DataSets.jl
 📄 DataTomlStorage.jl
 ...
```
"""
mutable struct BlobTree{Root} <: AbstractBlobTree
    root::Root
    path::RelPath
end

BlobTree(root) = BlobTree(root, RelPath())

function Base.show(io::IO, ::MIME"text/plain", tree::BlobTree)
    # TODO: Ideally we'd use
    # AbstractTrees.print_tree(io, tree, 1)
    # However, this is hard to use efficiently; we'd need to implement a lazy
    # `children()` for all our trees. It'd be much easier if
    # `AbstractTrees.has_children()` was used consistently upstream.
    println(io, "📂 Tree ", tree.path, " @ ", summary(tree.root))
    first = true
    for (name,x) in pairs(tree)
        if first
            first = false
        else
            print(io, '\n')
        end
        print(io, " ", isdir(x) ? '📁' : '📄', " ", name)
    end
end

function AbstractTrees.printnode(io::IO, tree::BlobTree)
    print(io, "📂 ",  basename(tree))
end

# getindex vs joinpath:
#  - getindex is about indexing the datastructure; therefore it looks in the
#    storage system to only return things which exist.
#  - joinpath just makes paths, not knowing whether they exist.
function Base.getindex(tree::BlobTree, path::RelPath)
    relpath = joinpath(tree.path, path)
    root = tree.root
    # TODO: Make this more efficient by moving this work to the storage backend?
    # Sort of like an equivalent of `stat`?
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
    getindex(tree, RelPath(name))
end


# Keys, values and iteration

"""
    children(tree::BlobTree)

Return an array of the children of `tree`. A child `x` may abstractly either be
another tree (`children(x)` returns a collection) or a file, where `children(x)`
returns `()`.
"""
function children(tree::BlobTree)
    [tree[RelPath([n])] for n in keys(tree)]
end

function Base.haskey(tree::BlobTree, path::AbstractString)
    haskey(tree, RelPath(path))
end

function Base.haskey(tree::BlobTree, path::RelPath)
    ispath(tree.root, joinpath(tree.path, path))
end

function Base.keys(tree::BlobTree)
    readdir(tree.root, tree.path)
end

function Base.pairs(tree::BlobTree)
    zip(keys(tree), children(tree))
end

function Base.values(tree::BlobTree)
    children(tree)
end


# Mutation

newdir(tree::BlobTree, path::AbstractString; kws...) =
    newdir(tree, RelPath(path); kws...)
newfile(tree::BlobTree, path::AbstractString; kws...) =
    newfile(tree, RelPath(path); kws...)
newfile(func::Function, tree::BlobTree, path::AbstractString; kws...) =
    newfile(func, tree, RelPath(path); kws...)
Base.delete!(tree::BlobTree, path::AbstractString) =
    delete!(tree, RelPath(path))

function _check_writeable(tree)
    if !iswriteable(tree.root)
        error("Attempt to write into a read-only tree with root $(tree.root)")
    end
end

function _check_new_item(tree, path, overwrite)
    _check_writeable(tree)
    if haskey(tree, path) && !overwrite
        error("Overwriting a path $path which already exists requires the keyword `overwrite=true`")
    end
end

"""
    newdir(tree, path; overwrite=false)

Create a new directory at tree[path] and return it. If `overwrite=true`, remove
any existing directory before creating the new one.

    newdir()

Create a new temporary `BlobTree` which can have files assigned into it and may
be assigned to a permanent location in a persistent `BlobTree`. If not assigned
to a permanent location, the temporary tree is cleaned up during garbage
collection.
"""
function newdir(tree::BlobTree, path::RelPath; overwrite=false)
    _check_new_item(tree, path, overwrite)
    p = joinpath(tree.path, RelPath(path))
    newdir(tree.root, p; overwrite=overwrite)
    return BlobTree(tree.root, p)
end

"""
    newfile(tree, path; overwrite=false)
    newfile(tree, path; overwrite=false) do io ...

Create a new file object in the `tree` at the given `path`. In the second form,
the open file `io` will be passed to the do block.

    newfile()

Create a new file which may be later assigned to a permanent location in a
tree. If not assigned to a permanent location, the temporary file is cleaned up
during garbage collection.

# Example

```
newfile(tree, "some/demo/path.txt") do io
    println(io, "Hi there!")
end
```
"""
function newfile(tree::BlobTree, path::RelPath; overwrite=false)
    _check_new_item(tree, path, overwrite)
    p = joinpath(tree.path, path)
    newfile(tree.root, p; overwrite=overwrite)
    return Blob(tree.root, p)
end

function newfile(func::Function, tree::BlobTree, path::RelPath; overwrite=false)
    _check_new_item(tree, path, overwrite)
    p = joinpath(tree.path, path)
    newfile(func, tree.root, p; overwrite=overwrite)
    return Blob(tree.root, p)
end


function Base.delete!(tree::BlobTree, path::RelPath)
    _check_writeable(tree)
    relpath = joinpath(tree.path, path)
    root = tree.root
    delete!(root, relpath)
end

function Base.open(f::Function, ::Type{BlobTree}, tree::BlobTree)
    f(tree)
end

@! function Base.open(::Type{BlobTree}, tree::BlobTree)
    tree
end

# Base.open(::Type{T}, file::Blob; kws...) where {T} = open(identity, T, file.root, file.path; kws...)


#-------------------------------------------------------------------------------
# Path manipulation

# TODO: Maybe deprecate these? Under the "datastructure-like" model, it seems wrong
# for a blob to know its name in the parent data structure.
Base.basename(tree::BlobTree) = basename(tree.path)
Base.abspath(tree::BlobTree) = AbsPath(tree.root, tree.path)

function Base.joinpath(tree::BlobTree, r::RelPath)
    AbsPath(tree.root, joinpath(tree.path, r))
end

function Base.joinpath(tree::BlobTree, s::AbstractString)
    AbsPath(tree.root, joinpath(tree.path, s))
end


#-------------------------------------------------------------------------------
# Deprecated
function Base.rm(tree::BlobTree; kws...)
    _check_writeable(tree)
    rm(tree.root, tree.path; kws...)
end

function Base.readdir(tree::BlobTree)
    readdir(tree.root, tree.path)
end

# Create files within a temporary directory.
function newdir(tree::BlobTree)
    Base.depwarn("""
        `newdir(::BlobTree)` for temporary trees is deprecated.
        Use the in-place version `newdir(::BlobTree, dirname)` instead.
        """,
        :newdir)
    newdir(tree.root)
end
function newfile(tree::BlobTree)
    Base.depwarn("""
        `newfile(::BlobTree)` for temporary trees is deprecated.
        Use the in-place version `newfile(::BlobTree, dirname)` instead.
        """,
        :newfile)
    newfile(tree.root)
end

