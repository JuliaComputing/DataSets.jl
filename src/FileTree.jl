# Many storage systems have tree-like indices.  Examples:
#
# Storage  Index           Data
# -------  -----------     ----------
# OS       filesystem      files
# Git      trees           blobs
# S3       keys            blobs
# HDF5     groups          typed data
# Zip      keys            blobs

import AbstractTrees: AbstractTrees, children

#-------------------------------------------------------------------------------
abstract type AbstractFileTree; end

# The tree API

# TODO: Should we have `istree` separate from `isdir`?
Base.isdir(x::AbstractFileTree) = true
Base.isfile(tree::AbstractFileTree) = false
Base.ispath(x::AbstractFileTree) = true

# Number of children is not known without a (potentially high-latency) call to
# an external resource
Base.IteratorSize(tree::AbstractFileTree) = Base.SizeUnknown()

function Base.iterate(tree::AbstractFileTree, state=nothing)
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
function showtree(io::IO, tree::AbstractFileTree; maxdepth=5)
    println(io, "📂 ", tree)
    _showtree(io, tree, "", maxdepth)
end

struct ShownTree
    tree
end
# Use a wrapper rather than defaulting to stdout so that this works in more
# functional environments such as Pluto.jl
showtree(tree::AbstractFileTree) = ShownTree(tree)

Base.show(io::IO, s::ShownTree) = showtree(io, s.tree)

function _showtree(io::IO, tree::AbstractFileTree, prefix, depth)
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

function Base.copy!(dst::AbstractFileTree, src::AbstractFileTree)
    for x in src
        xname = basename(x)
        if isdir(x)
            copy!(newdir(dst, xname), x)
        else
            open(x) do io_src
                newfile(dst, xname, overwrite=true) do io_dst
                    write(io_dst, io_src)
                end
            end
        end
    end
    return dst
end

Base.copy(src::AbstractFileTree) = copy!(newdir(), src)

#-------------------------------------------------------------------------------
"""
    File(root)
    File(root, relpath)

`File` represents the location of a collection of unstructured binary data. The
location is a path `relpath` relative to some `root` data resource.

A `File` can naturally be `open()`ed as a `Vector{UInt8}`, but can also be
mapped into the program as an `IO` byte stream, or interpreted as a `String`.

Files can be arranged into hierarchies "directories" via the `FileTree` type.
"""
mutable struct File{Root}
    root::Root
    path::RelPath
end

File(root) = File(root, RelPath())

Base.basename(file::File) = basename(file.path)
Base.abspath(file::File) = AbsPath(file.root, file.path)
Base.isdir(file::File) = false
Base.isfile(file::File) = true
Base.ispath(file::File) = true
Base.filesize(file::File) = filesize(file.root, file.path)

function Base.show(io::IO, ::MIME"text/plain", file::File)
    print(io, "📄 ", file.path, " @ ", summary(file.root))
end

function AbstractTrees.printnode(io::IO, file::File)
    print(io, "📄 ",  basename(file))
end

# Opening as Vector{UInt8} or as String defers to IO interface
function Base.open(f::Function, ::Type{Vector{UInt8}}, file::File)
    open(IO, file.root, file.path) do io
        f(read(io)) # TODO: use Mmap?
    end
end

function Base.open(f::Function, ::Type{String}, file::File)
    open(IO, file.root, file.path) do io
        f(read(io, String))
    end
end

# Default open-type for File is IO
Base.open(f::Function, file::File; kws...) = open(f, IO, file.root, file.path; kws...)

# Opening File as itself is trivial
function Base.open(f::Function, ::Type{File}, file::File)
    f(file)
end

# open with other types T defers to the underlying storage system
function Base.open(f::Function, ::Type{T}, file::File; kws...) where {T}
    open(f, T, file.root, file.path; kws...)
end

# ResourceContexts.jl - based versions of the above.

@! function Base.open(::Type{Vector{UInt8}}, file::File)
    @context begin
        # TODO: use Mmap?
        read(@! open(IO, file.root, file.path))
    end
end

@! function Base.open(::Type{String}, file::File)
    @context begin
        read(@!(open(IO, file.root, file.path)), String)
    end
end

# Default open-type for File is IO
@! function Base.open(file::File; kws...)
    @! open(IO, file.root, file.path; kws...)
end

# Opening File as itself is trivial
@! function Base.open(::Type{File}, file::File)
    file
end

# open with other types T defers to the underlying storage system
@! function Base.open(::Type{T}, file::File; kws...) where {T}
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

# Unscoped form of open for File
function Base.open(::Type{T}, file::File; kws...) where {T}
    @context begin
        result = @! open(T, file; kws...)
        @! ResourceContexts.detach_context_cleanup(result)
    end
end

# read() is also supported for `File`s
Base.read(file::File) = read(file.root, file.path)
Base.read(file::File, ::Type{T}) where {T} = read(file.root, file.path, T)


# Support for opening AbsPath
#
# TODO: Put this elsewhere?
function Base.open(f::Function, ::Type{T}, path::AbsPath; kws...) where {T}
    open(f, T, path.root, path.path; kws...)
end

Base.open(f::Function, path::AbsPath; kws...) = open(f, IO, path.root, path.path; kws...)


#-------------------------------------------------------------------------------
"""
    newdir()
    FileTree(root)

Create a `FileTree` which is a "directory tree" like hierarchy which may have
`File`s and `FileTree`s as children.  `newdir()` creates the tree in a
temporary directory on the local filesystem. Alternative `root`s may be
supplied which store the data elsewhere.

The tree implements the `AbstractTrees.children()` interface and may be indexed
with `/`-separated paths to traverse the hierarchy down to the leaves which are
of type `File`. Individual leaves may be `open()`ed as various Julia types.

# Operations on FileTree

`FileTree` has a largely dictionary-like interface:

* List keys (ie, file and directory names): `keys(tree)`
* List keys,value pairs:  `pairs(tree)`
* Query keys:            `haskey(tree)`
* Traverse the tree:     `tree["path"]`, `tree["multi/component/path"]`
* Add new content:       `newdir(tree, "path")`, `newfile(tree, "path")`
* Delete content:        `delete!(tree, "path")`

Iteration of FileTree iterates values (not key value pairs). This
has some benefits - for example, broadcasting processing across files in a
directory.

* Property access
  - `isdir()`, `isfile()` - determine whether a child of tree is a directory or file.
  - `filesize()` — size of `File` elements in a tree

# Example

Create a new temporary FileTree via the `newdir()` function and fill it with
files via `newfile()`:

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

Create a `FileTree` from a local directory with `DataSets.from_path()`:

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
mutable struct FileTree{Root} <: AbstractFileTree
    root::Root
    path::RelPath
end

FileTree(root) = FileTree(root, RelPath())

function Base.show(io::IO, ::MIME"text/plain", tree::FileTree)
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

function AbstractTrees.printnode(io::IO, tree::FileTree)
    print(io, "📂 ",  basename(tree))
end

# getindex vs joinpath:
#  - getindex is about indexing the datastructure; therefore it looks in the
#    storage system to only return things which exist.
#  - joinpath just makes paths, not knowing whether they exist.
function Base.getindex(tree::FileTree, path::RelPath)
    relpath = joinpath(tree.path, path)
    root = tree.root
    # TODO: Make this more efficient by moving this work to the storage backend?
    # Sort of like an equivalent of `stat`?
    if isdir(root, relpath)
        FileTree(root, relpath)
    elseif isfile(root, relpath)
        File(root, relpath)
    elseif ispath(root, relpath)
        AbsPath(root, relpath) # Not great?
    else
        error("Path $relpath @ $root doesn't exist")
    end
end

function Base.getindex(tree::FileTree, name::AbstractString)
    getindex(tree, RelPath(name))
end


# Keys, values and iteration

"""
    children(tree::FileTree)

Return an array of the children of `tree`. A child `x` may abstractly either be
another tree (`children(x)` returns a collection) or a file, where `children(x)`
returns `()`.
"""
function children(tree::FileTree)
    [tree[RelPath([n])] for n in keys(tree)]
end

function Base.haskey(tree::FileTree, path::AbstractString)
    haskey(tree, RelPath(path))
end

function Base.haskey(tree::FileTree, path::RelPath)
    ispath(tree.root, joinpath(tree.path, path))
end

function Base.keys(tree::FileTree)
    readdir(tree.root, tree.path)
end

function Base.pairs(tree::FileTree)
    zip(keys(tree), children(tree))
end

function Base.values(tree::FileTree)
    children(tree)
end


# Mutation

newdir(tree::FileTree, path::AbstractString; kws...) =
    newdir(tree, RelPath(path); kws...)
newfile(tree::FileTree, path::AbstractString; kws...) =
    newfile(tree, RelPath(path); kws...)
newfile(func::Function, tree::FileTree, path::AbstractString; kws...) =
    newfile(func, tree, RelPath(path); kws...)
Base.delete!(tree::FileTree, path::AbstractString) =
    Base.delete!(tree, RelPath(path))

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

Create a new FileTree ("directory") at tree[path] and return it. If
`overwrite=true`, remove any existing tree before creating the new one.
"""
function newdir(tree::FileTree, path::RelPath; overwrite=false)
    _check_new_item(tree, path, overwrite)
    p = joinpath(tree.path, path)
    newdir(tree.root, p; overwrite=overwrite)
    return FileTree(tree.root, p)
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
function newfile(tree::FileTree, path::RelPath; overwrite=false)
    _check_new_item(tree, path, overwrite)
    p = joinpath(tree.path, path)
    newfile(tree.root, p; overwrite=overwrite)
    return File(tree.root, p)
end

function newfile(func::Function, tree::FileTree, path::RelPath; overwrite=false)
    _check_new_item(tree, path, overwrite)
    p = joinpath(tree.path, path)
    newfile(func, tree.root, p; overwrite=overwrite)
    return File(tree.root, p)
end


function Base.delete!(tree::FileTree, path::RelPath)
    _check_writeable(tree)
    relpath = joinpath(tree.path, path)
    root = tree.root
    Base.delete!(root, relpath)
end

function Base.open(f::Function, ::Type{FileTree}, tree::FileTree)
    f(tree)
end

@! function Base.open(::Type{FileTree}, tree::FileTree)
    tree
end

# Base.open(::Type{T}, file::File; kws...) where {T} = open(identity, T, file.root, file.path; kws...)

function close_dataset(storage::Union{File,FileTree}, exc=nothing)
    close_dataset(storage.root)
end

# Utility functions
is_File_dtype(dtype)     = (dtype == "File"     || dtype == "Blob")
is_FileTree_dtype(dtype) = (dtype == "FileTree" || dtype == "BlobTree")

#-------------------------------------------------------------------------------
# Path manipulation

# TODO: Maybe deprecate these? Under the "datastructure-like" model, it seems wrong
# for a file to know its name in the parent data structure.
Base.basename(tree::FileTree) = basename(tree.path)
Base.abspath(tree::FileTree) = AbsPath(tree.root, tree.path)

function Base.joinpath(tree::FileTree, r::RelPath)
    AbsPath(tree.root, joinpath(tree.path, r))
end

function Base.joinpath(tree::FileTree, s::AbstractString)
    AbsPath(tree.root, joinpath(tree.path, s))
end


#-------------------------------------------------------------------------------
# Deprecated
function Base.rm(tree::FileTree; kws...)
    _check_writeable(tree)
    Base.depwarn("""
        `rm(::FileTree)` is deprecated.  Use `delete!(tree, path)` instead.
        """, :rm)
    rm(tree.root, tree.path; kws...)
end

function Base.readdir(tree::FileTree)
    readdir(tree.root, tree.path)
end

# Create files within a temporary directory.
function newdir(tree::FileTree)
    Base.depwarn("""
        `newdir(::FileTree)` for temporary trees is deprecated.
        Use the in-place version `newdir(::FileTree, dirname)` instead.
        """,
        :newdir)
    newdir(tree.root)
end
function newfile(tree::FileTree)
    Base.depwarn("""
        `newfile(::FileTree)` for temporary trees is deprecated.
        Use the in-place version `newfile(::FileTree, dirname)` instead.
        """,
        :newfile)
    newfile(tree.root)
end

