# Parsed file utility
"""
A cache of file content, parsed with an arbitrary parser function.

This is a modified and generalized version of `Base.CachedTOMLDict`.

Getting the value of the cache with `get_cache(f)` will automatically update
the parsed value whenever the file changes.
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

function get_cache(f::CachedParsedFile, allow_refresh=true)
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
            if !allow_refresh
                error("The file at $(f.path) was written externally")
            end
            @debug "Cache of file $(repr(f.path)) invalid, reparsing..."
            return f.d = f.parser(content)
        end
    end
    return f.d
end

function Base.show(io::IO, m::MIME"text/plain", f::CachedParsedFile)
    println(io, "Cache of file $(repr(f.path)) with value")
    show(io, m, get_cache(f))
end

# Parse Data.toml into DataProject which updates when the file does.
function parse_and_cache_project(proj, sys_path::AbstractString)
    sys_data_dir = dirname(sys_path)
    CachedParsedFile{DataProject}(sys_path) do content
        if isnothing(content)
            DataProject()
        else
            inner_proj = _load_project(String(content), sys_data_dir)
            for d in inner_proj
                d.project = proj
            end
            inner_proj
        end
    end
end

#------------------------------------------------------------------------------
abstract type AbstractTomlFileDataProject <: AbstractDataProject end

function Base.get(proj::AbstractTomlFileDataProject, name::AbstractString, default)
    get(get_cache(proj), name, default)
end

function Base.keys(proj::AbstractTomlFileDataProject)
    keys(get_cache(proj))
end

function Base.iterate(proj::AbstractTomlFileDataProject, state=nothing)
    # This is a little complex because we want iterate to work even if the
    # active project changes concurrently, which means wrapping up the initial
    # result of get_cache with the iterator state.
    if isnothing(state)
        cached_values = values(get_cache(proj))
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

Base.pairs(proj::AbstractTomlFileDataProject) = pairs(get_cache(proj))

data_drivers(proj::AbstractTomlFileDataProject) = data_drivers(get_cache(proj))

function config!(proj::AbstractTomlFileDataProject, dataset::DataSet; kws...)
    if data_project(dataset) !== proj
        error("dataset must belong to project")
    end
    # Here we accept the update independently of the project - Data.toml should
    # be able to manage any dataset config.
    config!(nothing, dataset; kws...)
    save_project(proj.path, get_cache(proj, false))
    return dataset
end

#-------------------------------------------------------------------------------
"""
Data project which automatically updates based on a TOML file on the local
filesystem.
"""
mutable struct TomlFileDataProject <: AbstractTomlFileDataProject
    path::String
    cache::CachedParsedFile{DataProject}
    function TomlFileDataProject(path::String)
        proj = new(path)
        proj.cache = parse_and_cache_project(proj, path)
        proj
    end
end

function get_cache(proj::TomlFileDataProject, refresh=true)
    get_cache(proj.cache, refresh)
end

function local_data_abspath(proj::TomlFileDataProject, relpath)
    return joinpath(dirname(proj.path), relpath)
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
    get_cache(proj)
    proj
end

function _active_project_data_toml(project_path=Base.active_project(false))
    isnothing(project_path) ?
        nothing :
        joinpath(dirname(project_path), "Data.toml")
end

function get_cache(proj::ActiveDataProject, allow_refresh=true)
    active_project = Base.active_project(false)
    if proj.active_project_path != active_project
        if !allow_refresh
            error("The current project path was changed")
        end
        # The unusual case: active project has changed.
        if isnothing(active_project)
            proj.cache = DataProject()
        else
            data_toml = _active_project_data_toml(active_project)
            # Need to re-cache
            proj.cache = parse_and_cache_project(proj, data_toml)
        end
        proj.active_project_path = active_project
    end
    proj.cache isa DataProject ? proj.cache : get_cache(proj.cache, allow_refresh)
end

function local_data_abspath(proj::ActiveDataProject, relpath)
    if isnothing(proj.active_project_path)
        error("No active project")
    end
    return joinpath(dirname(proj.active_project_path), relpath)
end

project_name(::ActiveDataProject) = _active_project_data_toml()

#-------------------------------------------------------------------------------

function _fill_template(toml_path, toml_str)
    # Super hacky templating for paths relative to the toml file.
    # We really should have something a lot nicer here...
    if Sys.iswindows()
        toml_path = replace(toml_path, '\\'=>'/')
    end
    if occursin("@__DIR__", toml_str)
        Base.depwarn("""
            Using @__DIR__ in Data.toml is deprecated. Use a '/'-separated
            relative path instead.""",
            :_fill_template)
        return replace(toml_str, "@__DIR__"=>toml_path)
    else
        return toml_str
    end
end

function _load_project(content::AbstractString, sys_data_dir)
    toml_str = _fill_template(sys_data_dir, content)
    config = TOML.parse(toml_str)
    load_project(config)
end

#-------------------------------------------------------------------------------
"""
    from_path(path)

Create a `DataSet` from a local filesystem path. The type of the dataset is
inferred as a blob or tree based on whether the local path is a file or
directory.
"""
function from_path(path::AbstractString)
    dtype = isfile(path) ? "File"     :
            isdir(path)  ? "FileTree" :
            nothing

    if isnothing(dtype)
        msg = ispath(path) ?
            "Unrecognized data at path \"$path\"" :
            "Path \"$path\" does not exist"
        throw(ArgumentError(msg))
    end

    path_key = Sys.isunix()    ? "unix_path" :
               Sys.iswindows() ? "windows_path" :
               error("Unknown system: cannot determine path type")

    conf = Dict(
        "name"=>make_valid_dataset_name(path),
        "uuid"=>string(uuid4()),
        "storage"=>Dict(
            "driver"=>"FileSystem",
            "type"=>dtype,
            path_key=>abspath(path),
        )
    )

    DataSet(conf)
end
