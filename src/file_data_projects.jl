# Parsed file utility
"""
A cache of file content, parsed with an arbitrary parser function.

This is a modified and generalized version of `Base.CachedTOMLDict`.

Getting the value of the cache with `f[]` will automatically update the parsed
value whenever the file changes.
"""
mutable struct CachedParsedFile{T}
    path::String
    inode::UInt64
    mtime::Float64
    size::Int64
    hash::UInt32
    d::Union{T,Nothing} # The cached value. `nothing` if file doesn't exist
    parser              # Parser function used to update the cached value
end

function CachedParsedFile{T}(parser::Function, path::String) where T
    s = stat(path)
    if ispath(s)
        content = read(path)
        crc32 = crc32c(content)
        d = parser(content)
    else
        crc32 = UInt32(0)
        d = nothing
    end
    return CachedParsedFile{T}(
        path,
        s.inode,
        s.mtime,
        s.size,
        crc32,
        d,
        parser,
    )
end

function Base.getindex(f::CachedParsedFile)
    s = stat(f.path)
    if !ispath(s)
        return nothing
    end
    time_since_cached = time() - f.mtime
    rough_mtime_granularity = 0.1 # seconds
    # In case the file is being updated faster than the mtime granularity,
    # and have the same size after the update we might miss that it changed. Therefore
    # always check the hash in case we recently created the cache.
    if #==# time_since_cached < rough_mtime_granularity ||
            s.inode != f.inode || s.mtime != f.mtime || f.size != s.size
        content = read(f.path)
        new_hash = crc32c(content)
        if new_hash != f.hash
            f.inode = s.inode
            f.mtime = s.mtime
            f.size = s.size
            f.hash = new_hash
            @debug "Cache of file $(repr(f.path)) invalid, reparsing..."
            return f.d = f.parser(content)
        end
    end
    return f.d
end

function Base.show(io::IO, m::MIME"text/plain", f::CachedParsedFile)
    println(io, "Cache of file $(repr(f.path)) with value")
    show(io, m, f[])
end

# Parse Data.toml into DataProject which updates when the file does.
function parse_and_cache_project(sys_path::AbstractString)
    sys_data_dir = dirname(sys_path)
    CachedParsedFile{DataProject}(sys_path) do content
        _load_project(String(content), sys_data_dir)
    end
end

#------------------------------------------------------------------------------
abstract type AbstractTomlFileDataProject <: AbstractDataProject end

function Base.get(proj::AbstractTomlFileDataProject, name::AbstractString, default)
    cache = _get_cached(proj)
    isnothing(cache) ? default : get(cache, name, default)
end

function Base.keys(proj::AbstractTomlFileDataProject)
    cache = _get_cached(proj)
    isnothing(cache) ? () : keys(cache)
end

function Base.iterate(proj::AbstractTomlFileDataProject, state=nothing)
    # This is a little complex because we want iterate to work even if the
    # active project changes concurrently, which means wrapping up the initial
    # result of _get_cached with the iterator state.
    if isnothing(state)
        cached_values = values(_get_cached(proj))
        if isnothing(cached_values)
            return nothing
        end
        wrapped_itr = iterate(cached_values)
    else
        (cached_values, wrapped_state) = state
        wrapped_itr = iterate(cached_values, wrapped_state)
    end
    if isnothing(wrapped_itr)
        return nothing
    else
        (data, wrapped_state) = wrapped_itr
        (data, (cached_values, wrapped_state))
    end
end

Base.pairs(proj::AbstractTomlFileDataProject) = pairs(_get_cached(proj))

#-------------------------------------------------------------------------------
"""
Data project which automatically updates based on a TOML file on the local
filesystem.
"""
mutable struct TomlFileDataProject <: AbstractTomlFileDataProject
    path::String
    cache::Union{Nothing,CachedParsedFile{DataProject}}
end

function TomlFileDataProject(path::String)
    proj = TomlFileDataProject(path, nothing)
    _get_cached(proj)
    proj
end

function _get_cached(proj::TomlFileDataProject)
    if isnothing(proj.cache) && isfile(proj.path)
        proj.cache = parse_and_cache_project(proj.path)
    end
    isnothing(proj.cache) ? nothing : proj.cache[]
end

project_name(proj::TomlFileDataProject) = proj.path

#------------------------------------------------------------------------------
"""
Data project, based on the location of the current explicitly selected Julia
Project.toml, as reported by `Base.active_project(false)`.

Several factors make the implementation a bit complicated:
  * The active project may change at any time without warning
  * The active project may be `nothing` when no explicit project is selected
  * There might be no Data.toml for the active project
  * The user can change Data.toml interactively and we'd like that to be
    reflected within the program.
"""
mutable struct ActiveDataProject <: AbstractTomlFileDataProject
    active_project_path::Union{Nothing,String} # Detects when Base.active_project changes
    cache::Union{Nothing,CachedParsedFile{DataProject}}
end

function ActiveDataProject()
    proj = ActiveDataProject(nothing, nothing)
    _get_cached(proj)
    proj
end

function _active_project_data_toml(project_path=Base.active_project(false))
    isnothing(project_path) ?
        nothing :
        joinpath(dirname(project_path), "Data.toml")
end

function _get_cached(proj::ActiveDataProject)
    active_project = Base.active_project(false)
    if proj.active_project_path != active_project
        # The unusual case: active project has changed.
        if isnothing(active_project)
            proj.cache = nothing
        else
            data_toml = _active_project_data_toml(active_project)
            # Need to re-cache
            proj.cache = parse_and_cache_project(data_toml)
        end
        proj.active_project_path = active_project
    end
    isnothing(proj.cache) ? nothing : proj.cache[]
end

project_name(::ActiveDataProject) = _active_project_data_toml()

#-------------------------------------------------------------------------------

# TODO: Deprecate this?
function load_project(path::AbstractPath)
    TomlFileDataProject(sys_abspath(abspath(path)))
end

function _load_project(content::AbstractString, sys_data_dir)
    toml_str = _fill_template(sys_data_dir, content)
    config = TOML.parse(toml_str)
    load_project(config)
end

