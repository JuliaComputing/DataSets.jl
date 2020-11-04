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
abstract type AbstractFileTree; end

# The tree API

# TODO: Should we have `istree` separate from `isdir`?
Base.isdir(x::AbstractFileTree) = true
Base.isfile(tree::AbstractFileTree) = false

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
    children(tree::AbstractFileTree)

Return an array of the children of `tree`. A child `x` may abstractly either be
another tree (`children(x)` returns a collection) or a file, where `children(x)`
returns `()`.

Note that this is subtly different from `readdir(path)` which returns relative
paths, or `readdir(path, join=true)` which returns absolute paths.
"""
function children(tree::AbstractFileTree)
    # TODO: Is dispatch to the root a correct default?
    children(tree.root, tree.path)
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

# An abstract type for trees which are actually rooted in the file system (in
# git terminology, there's a "working copy")
#
# TODO: Rename AbstractFilesystemRoot ?
abstract type AbstractFileTreeRoot end

# _joinpath generates and joins OS-specific _local filesystem paths_ from logical paths.
_joinpath(path::RelPath) = isempty(path.components) ? "" : joinpath(path.components...)
_abspath(path::AbsPath) = _abspath(path.root, path.path)

function _abspath(root::AbstractFileTreeRoot, path::RelPath)
    rootpath = _abspath(root)
    return isempty(path.components) ? rootpath : joinpath(rootpath, _joinpath(path))
end

# TODO: would it be better to express the following dispatch in terms of
# AbsPath{<:AbstractFileTreeRoot} rather than usin double dispatch?
Base.isdir(root::AbstractFileTreeRoot, path::RelPath) = isdir(_abspath(root, path))
Base.isfile(root::AbstractFileTreeRoot, path::RelPath) = isfile(_abspath(root, path))

function Base.open(f::Function, root::AbstractFileTreeRoot, path::RelPath;
                   write=false, read=!write, kws...)
    if !iswriteable(root) && write
        error("Error writing file at read-only path $path")
    end
    open(f, _abspath(root, path); read=read, write=write, kws...)
end

function Base.mkdir(root::AbstractFileTreeRoot, path::RelPath; kws...)
    if !iswriteable(root)
        error("Cannot make directory in read-only tree root at $(_abspath(p.root))")
    end
    mkdir(_abspath(root, path), args...)
    return FileTree(root, path)
end

Base.readdir(root::AbstractFileTreeRoot, path::RelPath) = readdir(_abspath(root, path))

struct FileTreeRoot <: AbstractFileTreeRoot
    path::String
    read::Bool
    write::Bool
end

function FileTreeRoot(path::AbstractString; write=false, read=true)
    path = abspath(path)
    if !isdir(path)
        throw(ArgumentError("$(repr(path)) must be a directory"))
    end
    FileTreeRoot(path, read, write)
end

iswriteable(root::FileTreeRoot) = root.write

_abspath(root::FileTreeRoot) = root.path

#-------------------------------------------------------------------------------
struct File{Root}
    root::Root
    path::RelPath
end

File(root) = File(root, RelPath())

Base.basename(file::File) = basename(file.path)
Base.abspath(file::File) = AbsPath(file.root, file.path)
Base.isdir(file::File) = false
Base.isfile(file::File) = true

function Base.show(io::IO, ::MIME"text/plain", file::File)
    print(io, "📄 ", file.path, " @ ", _abspath(file.root))
end

function AbstractTrees.printnode(io::IO, file::File)
    print(io, "📄 ",  basename(file))
end

#-------------------------------------------------------------------------------
struct FileTree{Root} <: AbstractFileTree
    root::Root
    path::RelPath
end

FileTree(root) = FileTree(root, RelPath())

function AbstractTrees.printnode(io::IO, tree::FileTree)
    print(io, "📂 ",  basename(tree))
end

function Base.show(io::IO, ::MIME"text/plain", tree::AbstractFileTree)
    # TODO: Ideally we'd use
    # AbstractTrees.print_tree(io, tree, 1)
    # However, this is hard to use efficiently; we'd need to implement a lazy
    # `children()` for all our trees. It'd be much easier if
    # `AbstractTrees.has_children()` was used consistently upstream.
    cs = children(tree)
    println(io, "📂 Tree ", tree.path, " @ ", tree.root)
    for (i, c) in enumerate(cs)
        print(io, " ", isdir(c) ? '📁' : '📄', " ", basename(c))
        if i != length(cs)
            print(io, '\n')
        end
    end
end

Base.basename(tree::FileTree) = basename(tree.path)
Base.abspath(tree::FileTree) = AbsPath(tree.root, tree.path)

# getindex vs joinpath:
#  - getindex about indexing the datastrcutre; therefore it looks in the
#    filesystem to only return things which exist.
#  - joinpath just makes paths, not knowing whether they exist.
function Base.getindex(tree::FileTree, path::RelPath)
    relpath = joinpath(tree.path, path)
    root = tree.root
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
    getindex(tree, joinpath(RelPath(), name))
end

function Base.joinpath(tree::FileTree, r::RelPath)
    AbsPath(tree.root, joinpath(tree.path, r))
end

function Base.joinpath(tree::FileTree, s::AbstractString)
    AbsPath(tree.root, joinpath(tree.path, s))
end

function Base.haskey(tree::FileTree, name::AbstractString)
    ispath(tree.root, joinpath(tree.path, name))
end

function Base.readdir(tree::FileTree)
    readdir(tree.root, tree.path)
end

function children(tree::FileTree)
    child_names = readdir(tree)
    [tree[c] for c in child_names]
end

Base.open(f::Function, file::File; kws...) = open(f, file.root, file.path; kws...)
Base.open(f::Function, path::AbsPath; kws...) = open(f, path.root, path.path; kws...)

Base.mkdir(p::AbsPath; kws...) = mkdir(p.root, p.path; kws...)


#--------------------------------------------------
# Almost-functional interface for creating file trees
mutable struct TempFilesystemRoot <: AbstractFileTreeRoot
    path::String
    isdir::Bool
    istemp::Bool
    function TempFilesystemRoot(path, isdir, istemp=true)
        root = new(path, isdir, istemp)
        finalizer(root) do r
            if r.istemp
                rm(r.path, recursive=r.isdir)
            end
        end
        return root
    end
end

iswriteable(root::TempFilesystemRoot) = true
_abspath(root::TempFilesystemRoot) = root.path

function newdir(ctx::AbstractFileTreeRoot=FileTreeRoot(tempdir(), write=true))
    # cleanup=false: we manage our own cleanup via the finalizer
    path = mktempdir(_abspath(ctx), cleanup=false)
    return FileTree(TempFilesystemRoot(path, true))
end
newdir(ctx::FileTree) = newdir(ctx.root)

function newfile(ctx::AbstractFileTreeRoot=FileTreeRoot(tempdir(), write=true))
    path, io = mktemp(_abspath(ctx), cleanup=false)
    close(io)
    return File(TempFilesystemRoot(path, false))
end
newfile(ctx::FileTree) = newfile(ctx.root)

function newfile(f::Function, ctx=FileTreeRoot(tempdir(), write=true))
    path, io = mktemp(_abspath(ctx), cleanup=false)
    try
        f(io)
    catch
        rm(path)
        rethrow()
    finally
        close(io)
    end
    return File(TempFilesystemRoot(path, false))
end

function Base.setindex!(tree::FileTree{<:AbstractFileTreeRoot},
                        value::Union{File{TempFilesystemRoot},FileTree{TempFilesystemRoot}},
                        name::AbstractString)
    if !iswriteable(tree.root)
        error("Attempt to move to a read-only tree $tree")
    end
    if !value.root.istemp
        type = value.root.isdir ? "directory" : "file"
        error("Attempted to root a temporary $type twice: $value")
    end
    destpath = _abspath(joinpath(tree, name))
    mv(_abspath(abspath(value)), destpath, force=true)
    value.root.path = destpath
    value.root.istemp = false
    return tree
end

# It's interesting to read about the linux VFS interface in regards to how the
# OS actually represents these things. For example
# https://stackoverflow.com/questions/36144807/why-does-linux-use-getdents-on-directories-instead-of-read

# For HDF5, need access to the attributes system
# (Files can have extended attributes too! Though it seems nobody really uses them?)
# https://www.tuxera.com/community/ntfs-3g-advanced/extended-attributes/
