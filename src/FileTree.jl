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

#-------------------------------------------------------------------------------
abstract type AbstractFileTree; end

isdir(x::AbstractFileTree) = true

# _joinpath generates and joins OS-specific _local filesystem paths_ from logical paths.
_joinpath(path::RelPath) = isempty(path.components) ? "" : joinpath(path.components...)

#-------------------------------------------------------------------------------
struct FileTreeRoot
    path::String
    readonly::Bool
end

function FileTreeRoot(path::AbstractString; readonly::Bool=true)
    path = abspath(path)
    if !isdir(path)
        throw(ArgumentError("$(repr(path)) must be a directory"))
    end
    FileTreeRoot(path, readonly)
end

_abspath(root::FileTreeRoot) = root.path
_abspath(ap::AbsPath{FileTreeRoot}) = joinpath(_abspath(ap.root), _joinpath(ap.path))

#-------------------------------------------------------------------------------
struct File
    root::FileTreeRoot
    path::RelPath
end

_abspath(file::File) = joinpath(_abspath(file.root), _joinpath(file.path))
Base.basename(file::File) = basename(file.path)
Base.isdir(file::File) = false
Base.isfile(file::File) = true

function Base.show(io::IO, ::MIME"text/plain", file::File)
    print(io, "📄 ", file.path, " @ ", _abspath(file.root))
end

#-------------------------------------------------------------------------------
struct FileTree <: AbstractFileTree
    root::FileTreeRoot
    path::RelPath
end

FileTree(root::FileTreeRoot) = FileTree(root, RelPath())

function Base.show(io::IO, ::MIME"text/plain", tree::AbstractFileTree)
    children = collect(tree)
    println(io, "📂 FileTree ", tree.path, " @ ", tree.root)
    for (i, c) in enumerate(children)
        print(io, " ", isdir(c) ? '📁' : '📄', " ", basename(c))
        if i != length(children)
            print(io, '\n')
        end
    end
end

Base.isfile(tree::FileTree) = false

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

Base.IteratorSize(tree::FileTree) = Base.SizeUnknown()
function Base.iterate(tree::FileTree, state=nothing)
    if state == nothing
        children = readdir(_abspath(tree))
        itr = iterate(children)
    else
        (children, cstate) = state
        itr = iterate(children, cstate)
    end
    if itr == nothing
        return nothing
    else
        (name, cstate) = itr
        (tree[name], (children, cstate))
    end
end

function Base.joinpath(tree::FileTree, r::RelPath)
    # Should this AbsPath be rooted at `tree` rather than `tree.root`?
    AbsPath(tree.root, joinpath(tree.path, r))
end

function Base.joinpath(tree::FileTree, s::AbstractString)
    AbsPath(tree.root, joinpath(tree.path, s))
end


# Mutation

function Base.open(func::Function, f::File; write=false, read=!write)
    if f.root.readonly && write
        error("Error writing file at read-only path $f")
    end
    open(func, _abspath(f); read=read, write=write)
end

function Base.open(func::Function, p::AbsPath; write=false, read=!write)
    if p.root.readonly && write
        error("Error writing file at read-only path $p")
    end
    open(func, _abspath(p); read=read, write=write)
end

function Base.mkdir(p::AbsPath, args...)
    if p.root.readonly
        error("Cannot make directory in read-only tree root at $(_abspath(tree.root))")
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
