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

function set_cache(f::CachedParsedFile, content::AbstractString)
    mktemp(dirname(f.path)) do tmppath, tmpio
        write(tmpio, content)
        close(tmpio)
        # Uses mktemp() + mv() to atomically overwrite the file
        mv(tmppath, f.path, force=true)
    end
    s = stat(f.path)
    f.inode = s.inode
    f.mtime = s.mtime
    f.size = s.size
    f.hash = sha1(content)
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
                # Hack; we steal ownership from the DataProject here.
                # What's a better way to do this?
                setfield!(d, :project, proj)
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
    set_cache(proj, project_toml(get_cache(proj, false)))
    return dataset
end

function create(proj::AbstractTomlFileDataProject, name;
        # One of the following required
        source::Union{Nothing,DataSet}=nothing,
        driver::Union{Nothing,AbstractString}=nothing,
        linked = false,
        # Descriptive metadata
        kws...
    )

    if isnothing(project_name(proj))
        return nothing
    end

    if haskey(proj, name)
        throw(ArgumentError("DataSet named \"$name\" already exists in project."))
    end

    driver = linked && !isnothing(source) ? _find_driver(source) :
             !isnothing(driver)           ? _find_driver(driver) :
             default_driver(proj)

    if linked
        storage = deepcopy(source.storage)
    else
        storage = create_storage(proj, driver, name; source=source, kws...)
    end

    conf = Dict(
        "name"=>name,
        "uuid"=>string(uuid4()),
        "storage"=>storage
    )

    conf["linked"] = linked
    for (k,v) in kws
        conf[string(k)] = v
    end

    ds = DataSet(conf)
    proj[ds.name] = ds
    return ds
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

function set_cache(proj::TomlFileDataProject, content::AbstractString)
    set_cache(proj.cache, content)
end

function local_data_abspath(proj::TomlFileDataProject, relpath)
    return joinpath(dirname(proj.path), relpath)
end

project_name(proj::TomlFileDataProject) = proj.path

function Base.setindex!(proj::TomlFileDataProject, data::DataSet, name::AbstractString)
    p = get_cache(proj)
    p[name] = data
    save_project(proj.path, p)
end

function delete(proj::TomlFileDataProject, name::AbstractString)
    # FIXME: Make this safe for concurrent use in-process
    # (or better, between processes?)
    p = get_cache(proj)

    ds = dataset(p, name)
    # Assume all datasets which don't have the "linked" property are linked.
    # This prevents us accidentally deleting data.
    if get(ds, "linked", true)
        @info "Linked dataset is preserved on data storage" name
    else
        driver = _find_driver(ds)
        delete_storage(proj, driver, ds)
    end

    delete(p, name)
    save_project(proj.path, p)
end

#-------------------------------------------------------------------------------
default_driver(proj::AbstractTomlFileDataProject) = FileSystemDriver()

project_root_path(proj) = error("No local path for data project type $(typeof(proj))")
project_root_path(proj::TomlFileDataProject) = dirname(proj.path)


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

function set_cache(proj::ActiveDataProject, content::AbstractString)
    if proj.cache isa DataProject
        error("No current active project")
    else
        set_cache(proj.cache, content)
    end
end

function local_data_abspath(proj::ActiveDataProject, relpath)
    if isnothing(proj.active_project_path)
        error("No active project")
    end
    return joinpath(dirname(proj.active_project_path), relpath)
end

project_name(::ActiveDataProject) = _active_project_data_toml()

#-------------------------------------------------------------------------------

function _fill_template(toml_str)
    if occursin("@__DIR__", toml_str)
        Base.depwarn("""
            Using @__DIR__ in Data.toml is deprecated. Use a '/'-separated
            relative path instead.""",
            :_fill_template)
        return replace(toml_str, "@__DIR__"=>".")
    else
        return toml_str
    end
end

function _load_project(content::AbstractString, sys_data_dir)
    toml_str = _fill_template(content)
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
