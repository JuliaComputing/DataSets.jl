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

export @path_str

abstract type AbstractFileTree; end

"""
    A tool to write relative path literals succinctly
"""
macro path_str(str)
    # FIXME: This is system-independent which is good, but root paths can be
    # truly weird, especially on windows, so this split may not make sense.
    components = Vector{Any}(split(str, '/'))
    for i in eachindex(components)
        if startswith(components[i], '$')
            components[i] = esc(Symbol(components[i][2:end]))
        end
    end
    :(joinpath($(components...)))
end


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

Base.joinpath(root::FileTreeRoot, path::AbstractString) = joinpath(root.path, path)
Base.abspath(root::FileTreeRoot) = root.path


#-------------------------------------------------------------------------------
struct File
    root::FileTreeRoot
    path::String
end

Base.abspath(file::File) = joinpath(file.root, file.path)
Base.basename(file::File) = basename(file.path)
Base.isdir(file::File) = false
Base.isfile(file::File) = true

function Base.show(io::IO, ::MIME"text/plain", file::File)
    print(io, "📄 ", repr(file.path), " @ ", abspath(file.root))
end


#-------------------------------------------------------------------------------
struct FileTree <: AbstractFileTree
    root::FileTreeRoot
    path::String
end

FileTree(path::AbstractString) = FileTree(FileTreeRoot(path), ".")
FileTree(root::FileTreeRoot) = FileTree(root, ".")

function Base.show(io::IO, ::MIME"text/plain", tree::AbstractFileTree)
    children = collect(tree)
    println(io, "FileTree ", repr(tree.path), " @ ", abspath(tree.root))
    for (i, c) in enumerate(children)
        # Cute version using the 📁 (or 📂?) symbol.
        print(io, " ", isdir(c) ? '📁' : '📄', " ", basename(c))
        if i != length(children)
            print(io, '\n')
        end
    end
end

Base.isdir(tree::FileTree) = true
Base.isfile(tree::FileTree) = false

Base.basename(tree::FileTree) = basename(tree.path)

# NB: abspath is a handy convenience for trees which are explicitly file-based,
# but can't be part of a general API...
Base.abspath(tree::FileTree) = joinpath(tree.root, tree.path)

Base.getindex(tree::FileTree, name::AbstractString) = joinpath(tree, name)
# ^ TODO: getindex with relative path by indexing with a path type?

Base.IteratorSize(tree::FileTree) = Base.SizeUnknown()
function Base.iterate(tree::FileTree, state=nothing)
    if state == nothing
        children = readdir(abspath(tree))
        itr = iterate(children)
    else
        (children, cstate) = state
        itr = iterate(children, cstate)
    end
    if itr == nothing
        return nothing
    else
        (name, cstate) = itr
        (joinpath(tree, name), (children, cstate))
    end
end

function Base.joinpath(tree::FileTree, xs...)
    # TODO: Make this separate from getindex?
    #
    # Here's the distinction:
    #  - getindex about indexing the datastrcutre; therefore it looks in the
    #    filesystem to only return things which exist.
    #  - joinpath just makes paths, not knowing whether they exist.
    p = joinpath(tree.path, joinpath(xs...))
    absp = joinpath(tree.root, p)
    if isdir(absp)
        FileTree(tree.root, p)
    elseif isfile(absp)
        File(tree.root, p)
    elseif islink(absp)
        # TODO - this isn't actually right - broken symlinks are neither files
        # nor directorys - they may be better returned as a path type?
        File(tree.root, p)
    else
        error("Path $p doesn't exist")
    end
end


# It's interesting to read about the linux VFS interface in regards to how the
# OS actually represents these things. For example
# https://stackoverflow.com/questions/36144807/why-does-linux-use-getdents-on-directories-instead-of-read

# For HDF5, need access to the attributes system
# (Files can have extended attributes too! Though it seems nobody really uses them?)
# https://www.tuxera.com/community/ntfs-3g-advanced/extended-attributes/
