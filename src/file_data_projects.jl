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
    hash::Vector{UInt8}
    d::T      # The cached value.
    parser    # Parser function used to update the cached value
end

function CachedParsedFile{T}(parser::Function, path::String) where T
    s = stat(path)
    if ispath(s)
        content = read(path)
        hash = sha1(content)
    else
        hash = UInt8[]
        content = nothing
    end
    d = parser(content)
    return CachedParsedFile{T}(
        path,
        s.inode,
        s.mtime,
        s.size,
        hash,
        d,
        parser,
    )
end

function Base.getindex(f::CachedParsedFile)
    s = stat(f.path)
    time_since_cached = time() - f.mtime
    rough_mtime_granularity = 0.1 # seconds
    # In case the file is being updated faster than the mtime granularity,
    # and have the same size after the update we might miss that it changed. Therefore
    # always check the hash in case we recently created the cache.
    if #==# time_since_cached < rough_mtime_granularity ||
            s.inode != f.inode || s.mtime != f.mtime || f.size != s.size
        if ispath(s)
            content = read(f.path)
            new_hash = sha1(content)
        else
            content = nothing
            new_hash = UInt8[]
        end
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
        if isnothing(content)
            DataProject()
        else
            _load_project(String(content), sys_data_dir)
        end
    end
end

#------------------------------------------------------------------------------
abstract type AbstractTomlFileDataProject <: AbstractDataProject end

function Base.get(proj::AbstractTomlFileDataProject, name::AbstractString, default)
    get(_get_cached(proj), name, default)
end

function Base.keys(proj::AbstractTomlFileDataProject)
    keys(_get_cached(proj))
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
    cache::CachedParsedFile{DataProject}
end

function TomlFileDataProject(path::String)
    cache = parse_and_cache_project(path)
    TomlFileDataProject(path, cache)
end

function _get_cached(proj::TomlFileDataProject)
    proj.cache[]
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
    cache::Union{DataProject,CachedParsedFile{DataProject}}
end

function ActiveDataProject()
    proj = ActiveDataProject(nothing, DataProject())
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
            proj.cache = DataProject()
        else
            data_toml = _active_project_data_toml(active_project)
            # Need to re-cache
            proj.cache = parse_and_cache_project(data_toml)
        end
        proj.active_project_path = active_project
    end
    proj.cache isa DataProject ? proj.cache : proj.cache[]
end

project_name(::ActiveDataProject) = _active_project_data_toml()

#-------------------------------------------------------------------------------

function _load_project(content::AbstractString, sys_data_dir)
    toml_str = _fill_template(sys_data_dir, content)
    config = TOML.parse(toml_str)
    load_project(config)
end

#-------------------------------------------------------------------------------
struct DirectoryDataProject <: AbstractDataProject
    path::String
    name_map::Dict{String,String}
    project::DataProject
end

function _sanitize_name(name)
    replace(name, r"[^[:alnum:]_]"=>"_")
end

function DirectoryDataProject(path::AbstractString)
    path = abspath(path)
    name_map = Dict{String,String}()
    project = DataProject()
    for name in readdir(path)
        sname = _sanitize_name(name)
        sname_orig = sname
        sname_idx = 1
        while haskey(name_map, sname)
            sname = "$(sname_orig)_$sname_idx"
        end
        name_map[name] = sname
        st = stat(joinpath(path,name))
        type = isfile(st) ? "Blob" :
               isdir(st)  ? "BlobTree" :
               nothing
        if isnothing(type)
            @warn "Ignoring directory entry which isn't a file or directory" path
            continue
        end
        conf = Dict("name"=>sname,
                    "description"=>"Autogenerated file $repr(name)",
                    "uuid"=>string(uuid4()),
                    "storage"=>Dict(
                        "driver"=>"FileSystem",
                        "type"=>type,
                        "path"=>joinpath(path, name)
                    )
               )
        link_dataset(project, sname=>DataSet(conf))
    end
    DirectoryDataProject(path, name_map, project)
end

function Base.keys(project::DirectoryDataProject)
    # TODO: Reload keys where necessary
    keys(project.project)
end

function Base.get(project::DirectoryDataProject, name::AbstractString, default)
    # TODO: Reload keys where necessary
    get(project.project, name, default)
end

