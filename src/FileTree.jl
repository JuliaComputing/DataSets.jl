# Many datasets have tree-like indices.  Examples:
#
# * OS:  directories  files
# * Git: trees        blobs
# * S3:  prefixes     blobs
# * HDF5 group        data
#

abstract type AbstractFileTree; end

#=
function _joinpath_components(ex)
    if ex isa Symbol
        string(ex)
    elseif ex isa Expr
        if ex.head == :call
            if ex.args[1] in (:/, :\)
                reduce(vcat, map(_joinpath_components, ex.args[2:end]))
            elseif ex.args[1] == :-
                join(map(_joinpath_components, ex.args), ".")
            else
                error("Path component $ex not supported")
            end
        elseif ex.head == :(.) && ex.args[1] isa Symbol && ex.args[2] isa QuoteNode
            string(ex.args[1]) * "." * string(ex.args[2].value)
        else
            error("Path component $ex not supported")
        end
    else
        ex
    end
end

macro joinpath(ex)
    :(joinpath($(_joinpath_components(ex)...)))
end
=#

macro path_str(str)
    components = Vector{Any}(splitpath(str))
    for i in eachindex(components)
        if startswith(components[i], '$')
            components[i] = esc(Symbol(components[i][2:end]))
        end
    end
    :(joinpath($(components...)))
end

# TODO: Use FilePathsBase?
struct FilePath
    path::String
end

struct DirEntry
    isdir::Bool
    name::String
end

struct FileTree <: AbstractFileTree
    path::String
    children::Vector{DirEntry}
end

function FileTree(path::AbstractString)
    # Note: Would get the isdir() for free if using libuv directly
    childnames = readdir(path)
    children = [DirEntry(isdir(joinpath(path, name)), name) for name in childnames]
    FileTree(path, children)
end

function Base.show(io::IO, tree::FileTree)
    print(io, FileTree, '(', repr(tree.path), " #= ", length(tree), " children =#)")
end

function Base.show(io::IO, ::MIME"text/plain", tree::FileTree)
    println(io, "FileTree at ", repr(tree.path), " with ", length(tree), " children:")
    for (i, c) in enumerate(tree.children)
        # Cute version using the 📁 (or 📂?) symbol.
        # Should we tone it down or go even further using '📄'?
        print(io, " ", c.isdir ? "📁" : "  ", " ", c.name)
        if i != length(tree.children)
            print(io, '\n')
        end
    end
end

Base.IteratorSize(tree::FileTree) = Base.HasLength()
Base.length(tree::FileTree) = length(tree.children)

function Base.getindex(tree::FileTree, i::Int)
    child = tree.children[i]
    path = joinpath(tree.path, child.name)
    # Could do caching here...
    return child.isdir ? FileTree(path) : FilePath(path)
end

function Base.getindex(tree::FileTree, name::AbstractString)
    for (i, c) in enumerate(tree.children)
        if c.name == name
            return tree[i]
        end
    end
    throw(KeyError(name))
end

function Base.iterate(tree::FileTree, state = 1)
    state <= length(tree.children) || return nothing
    (tree[state], state+1)
end

function Base.joinpath(tree::FileTree, xs...)
    p = joinpath(tree.path, joinpath(xs...))
    if isdir(p)
        FileTree(p)
    elseif isfile(p)
        FilePath(p)
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
