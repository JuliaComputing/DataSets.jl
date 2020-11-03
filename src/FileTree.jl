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

# _joinpath generates and joins OS-specific _local filesystem paths_ from logical paths.
_joinpath(path::RelPath) = isempty(path.components) ? "" : joinpath(path.components...)

struct FileTreeRoot
    path::String
    file_opener
    read::Bool
    write::Bool
end

function FileTreeRoot(path::AbstractString; write=false, read=true)
    path = abspath(path)
    if !isdir(path)
        throw(ArgumentError("$(repr(path)) must be a directory"))
    end
    FileTreeRoot(path, open, read, write)
end

_abspath(root::FileTreeRoot) = root.path
_abspath(ap::AbsPath{FileTreeRoot}) = joinpath(_abspath(ap.root), _joinpath(ap.path))

Base.open(root::FileTreeRoot) = FileTree(root)
Base.close(root::FileTreeRoot) = nothing

#-------------------------------------------------------------------------------
struct File{Root}
    root::Root
    path::RelPath
end

_abspath(file::File) = joinpath(_abspath(file.root), _joinpath(file.path))
Base.basename(file::File) = basename(file.path)
Base.isdir(file::File) = false
Base.isfile(file::File) = true

function Base.show(io::IO, ::MIME"text/plain", file::File)
    print(io, "📄 ", file.path, " @ ", _abspath(file.root))
end

function AbstractTrees.printnode(io::IO, tree::File)
    print(io, "📄 ",  basename(tree))
end

#-------------------------------------------------------------------------------
struct FileTree{Root} <: AbstractFileTree
    root::Root
    path::RelPath
end

FileTree(root) = FileTree(root, RelPath())
Base.close(root::AbstractFileTree) = close(tree.root)

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

_abspath(tree::FileTree) = joinpath(_abspath(tree.root), _joinpath(tree.path))

# getindex vs joinpath:
#  - getindex about indexing the datastrcutre; therefore it looks in the
#    filesystem to only return things which exist.
#  - joinpath just makes paths, not knowing whether they exist.
function Base.getindex(tree::FileTree, path::RelPath)
    relpath = joinpath(tree.path, path)
    absp = joinpath(_abspath(tree.root), _joinpath(relpath))
    if isdir(absp)
        FileTree(tree.root, relpath)
    elseif isfile(absp)
        File(tree.root, relpath)
    elseif islink(absp)
        AbsPath(tree.root, relpath)
    else
        error("Path $absp doesn't exist")
    end
end

function Base.getindex(tree::FileTree, name::AbstractString)
    getindex(tree, joinpath(RelPath(), name))
end

function Base.haskey(tree::FileTree{FileTreeRoot}, name::AbstractString)
    ispath(_abspath(joinpath(tree,name)))
end

function children(tree::FileTree{FileTreeRoot})
    fs_path = _abspath(tree)
    child_names = readdir(fs_path)
    [tree[c] for c in child_names]
end

function Base.joinpath(tree::FileTree, r::RelPath)
    # Should this AbsPath be rooted at `tree` rather than `tree.root`?
    AbsPath(tree.root, joinpath(tree.path, r))
end

function Base.joinpath(tree::FileTree, s::AbstractString)
    AbsPath(tree.root, joinpath(tree.path, s))
end


# Mutation

function Base.open(func::Function, f::File{FileTreeRoot}; write=false, read=!write)
    if !f.root.write && write
        error("Error writing file at read-only path $f")
    end
    f.root.file_opener(func, _abspath(f); read=read, write=write)
end

function Base.open(func::Function, p::AbsPath{FileTreeRoot}; write=false, read=!write)
    if !p.root.write && write
        error("Error writing file at read-only path $p")
    end
    p.root.file_opener(func, _abspath(p); read=read, write=write)
end

function Base.mkdir(p::AbsPath{FileTreeRoot}, args...)
    if !p.root.write
        error("Cannot make directory in read-only tree root at $(_abspath(p.root))")
    end
    mkdir(_abspath(p), args...)
    return FileTree(p.root, p.path)
end

#function Base.rm(tree::FileTree; recursive=false)
#end

# It's interesting to read about the linux VFS interface in regards to how the
# OS actually represents these things. For example
# https://stackoverflow.com/questions/36144807/why-does-linux-use-getdents-on-directories-instead-of-read

# For HDF5, need access to the attributes system
# (Files can have extended attributes too! Though it seems nobody really uses them?)
# https://www.tuxera.com/community/ntfs-3g-advanced/extended-attributes/
