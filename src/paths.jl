export @path_str

#-------------------------------------------------------------------------------

"""
    path"relative/path/to/resource"

A `RelPath` is a *key* into a hierarchical string-indexed tree datastructure,
with each component indexing one level of the hierarchy.

As a key, the resource referred to by a path may or may not exist.
Conversely, `FileTree` and `File` refer to the actual data stored with a given
key.
"""
struct RelPath
    components::Vector{String}
end

RelPath(::AbstractString) = error("RelPath(::String) is not defined to avoid ambiguities between operating systems. Use  the `path\"...\"` string macro for path literals.")
RelPath(components::AbstractVector=String[]) = RelPath(convert(Vector{String}, components))

# Path manipulation.
function Base.joinpath(path::RelPath, xs::AbstractString...)
    for x in xs
        if '/' in x || '\\' in x
            error("Path components cannot contain '/' or '\\' (got $(repr(x)))")
        end
    end
    RelPath(vcat(path.components, xs...))
end
Base.joinpath(path::RelPath, xs::RelPath...) =
    RelPath(vcat(path.components, [x.components for x in xs]...))

Base.isempty(p::RelPath) = isempty(p.components)

Base.basename(path::RelPath) = isempty(path) ? "" : last(path.components)
Base.dirname(path::RelPath) = RelPath(path.components[1:end-1])

Base.print(io::IO, p::RelPath) = print(io, join(p.components, '/'))
Base.show(io::IO,  p::RelPath) = print(io, "path", repr(string(p)))


function Base.startswith(a::RelPath, b::RelPath)
    return length(a.components) >= length(b.components) &&
           a.components[1:length(b.components)] == b.components
end

function Base.:(==)(p1::RelPath, p2::RelPath)
    return p1.components == p2.components
end

function Base.isless(p1::RelPath, p2::RelPath)
    for (a,b) in zip(p1.components, p2.components)
        isless(a, b) && return true
        isless(b, a) && return false
    end
    return length(p1.components) < length(p2.components)
end

macro path_str(str)
    # For OS-independence, just allow / and \ as equivalent; normalize to '/' when printing.
    # As a side effect, this disallows use of \ in individual *path literal
    # components* on unix. But that would be perverse, right?
    components = isempty(str) ? [] : split(str, ('/', '\\'))
    RelPath(components)
end

#=
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
=#

#-------------------------------------------------------------------------------
"""
    An AbsPath is the *key* into a hierarchical tree index, relative to some root.

As a *key*, the resource pointed to by this key may or may not exist.
"""
struct AbsPath{Root}
    root::Root
    path::RelPath
end

AbsPath(root, path::AbstractString) = AbsPath(root, RelPath(path))

function Base.show(io::IO, ::MIME"text/plain", path::AbsPath)
    print(io, "AbsPath ", path.path, " @ ", path.root)
end

Base.mkdir(p::AbsPath; kws...) = mkdir(p.root, p.path; kws...)
Base.rm(p::AbsPath; kws...) = rm(p.root, p.path; kws...)

