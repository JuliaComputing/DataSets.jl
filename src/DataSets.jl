module DataSets

using UUIDs
using TOML
using CRC32c

# using CSV, CodecZlib
# using HDF5

export DataSet, dataset, @datafunc, @datarun
export Blob, BlobTree, newfile, newdir

include("paths.jl")

#-------------------------------------------------------------------------------

#=
# This type for mapping data
struct DataMap
    type::String
    parameters
end

function read_toml(::Type{DataMap}, t)
    DataMap(t["type"], collect(get(t, "parameters", [])))
end
=#

"""
A `DataSet` is a metadata overlay for data held locally or remotely which is
unopinionated about the underlying storage mechanism.

The data in a `DataSet` has a type which implies an index; the index can be
used to partition the data for processing.
"""
struct DataSet
    # For now, the representation `conf` contains data read directly from the
    # TOML. Once the design has settled we might get some explicit fields and
    # do validation.
    uuid::UUID     # Unique identifier for the dataset. Use uuid4() to create these.
    conf

    function DataSet(conf)
        _check_keys(conf, DataSet, ["uuid", "storage", "name"])
        check_dataset_name(conf["name"])
        new(UUID(conf["uuid"]), conf)
    end

    #=
    name::String # Default name for convenience.
                         # The binding to an actual name is managed by the data
                         # project.
    storage        # Storage config and driver definition
    maps::Vector{DataMap}

    # Generic dictionary of other properties... for now. Required properties
    # will be moved
    _other::Dict{Symbol,Any}

    #storage_id     # unique identifier in storage backend, if it exists
    #owner          # Project or user who owns the data
    #description::String
    #type           # Some representation of the type of data?
    #               # An array, blob, table, tree, etc
    #cachable::Bool # Can the data be cached?  It might not for data governance
    #               # reasons or it might change commonly.
    ## A set of identifiers
    #tags::Set{String}
    =#
end

function _check_keys(toml, context, keys)
    missed_keys = filter(k->!haskey(toml, k), keys)
    if !isempty(missed_keys)
        @info toml
        error("""
              Missing expected keys:
              $missed_keys

              In TOML fragment:
              $(sprint(TOML.print,toml))
              """)
    end
end

function check_dataset_name(name::AbstractString)
    # Disallow punctuation in DataSet names for now, as it may be needed as
    # delimiters in data-related syntax (eg, for the data REPL).
    if !occursin(r"^[[:alpha:]][[:alnum:]_]*$", name)
        error("DataSet name must start with a letter, and can only contain letters, numbers or underscores; got \"$name\"")
    end
end

function read_toml(::Type{DataSet}, toml)
    DataSet(toml)
end

# Hacky thing until we figure out which fields DataSet should actually have.
function Base.getproperty(d::DataSet, name::Symbol)
    if name in fieldnames(DataSet)
        return getfield(d, name)
    else
        getfield(d, :conf)[string(name)]
    end
end

function Base.show(io::IO, d::DataSet)
    print(io, DataSet, " $(d.name) $(d.uuid)")
end

function Base.show(io::IO, ::MIME"text/plain", d::DataSet)
    TOML.print(io, d.conf)
end


#-------------------------------------------------------------------------------
"""
Subtypes of `AbstractDataProject` have the interface

  - List   - `keys`, `iterate`
  - Search - `get(project, dataset_name, default)`
  - Search - `haskey`, `getindex` - implemented in terms of `get` by default.
  - Fancy search - `dataset` - implemented in terms of `get`

"""
abstract type AbstractDataProject end

function Base.getindex(proj::AbstractDataProject, name::AbstractString)
    data = get(proj, name, nothing)
    data != nothing || error("Dataset $(repr(name)) not found")
    data
end

function dataset(proj::AbstractDataProject, name::AbstractString)
    # Non-fancy search... for now :)
    proj[name]
end

function Base.haskey(proj::AbstractDataProject, name::AbstractString)
    get(proj, name, nothing) !== nothing
end

"""
    DataProject

A concrete data project is a collection of DataSets with associated names.
Names are unique within the project.
"""
struct DataProject <: AbstractDataProject
    datasets::Dict{String,DataSet}
end

DataProject() = DataProject(Dict{String,DataSet}())

function _fill_template(toml_path, toml_str)
    # Super hacky templating for paths relative to the toml file.
    # We really should have something a lot nicer here...
    if Sys.iswindows()
        toml_path = replace(toml_path, '\\'=>'/')
    end
    toml_str = replace(toml_str, "@__DIR__"=>toml_path)
end

"""
Current version of the data configuration format, as reflected in the
Data.toml data_config_version key.
"""
const CURRENT_DATA_CONFIG_VERSION = 0

"""
    load_project!([path | config_dict])

Load a data project from a `path::AbstractPath` referring to a TOML file, or
from a `config_dict` which should be in the Data.toml format.

See also [`load_project!`](@ref).
"""
function load_project(config::AbstractDict)
    format_ver = config["data_config_version"]
    if format_ver > CURRENT_DATA_CONFIG_VERSION
        error("data_config_version=$format_ver is newer than supported")
    end
    proj = DataProject()
    for data_toml in config["datasets"]
        dataset = read_toml(DataSet, data_toml)
        link_dataset(proj, dataset.name => dataset)
    end
    proj
end

function load_project(path::AbstractPath)
    path = abspath(path)
    content = read(path, String)
    _load_project(content, dirname(sys_abspath(path)))
end

function _load_project(content::AbstractString, sys_data_dir)
    toml_str = _fill_template(sys_data_dir, content)
    config = TOML.parse(toml_str)
    load_project(config)
end

function link_dataset(proj::DataProject, (name,data)::Pair)
    proj.datasets[name] = data
end

link_dataset(proj::DataProject, d::DataSet) = link_dataset(proj, d.name=>d)

function unlink_dataset(proj::DataProject, name::AbstractString)
    if !haskey(proj.datasets, name)
        throw(ArgumentError("No dataset \"$name\" in data project"))
    end
    d = proj.datasets[name]
    delete!(proj.datasets, name)
    d
end

function Base.get(proj::DataProject, name::AbstractString, default)
    get(proj.datasets, name, default)
end

Base.keys(proj::DataProject) = keys(proj.datasets)

Base.iterate(proj::DataProject) = iterate(proj.datasets)
Base.iterate(proj::DataProject, state) = iterate(proj.datasets, state)

function Base.show(io::IO, ::MIME"text/plain", proj::DataProject)
    if isempty(proj.datasets)
        print(io, "DataProject (empty)")
        return
    end
    println(io, "DataProject:")
    sorted = sort(collect(proj.datasets), by=first)
    maxwidth = maximum(textwidth.(first.(sorted)))
    for (i, (name, data)) in enumerate(sorted)
        pad = maxwidth - textwidth(name)
        print(io, "  ", name, ' '^pad, " => ", data.uuid)
        if i < length(sorted)
            println(io)
        end
    end
end

include("file_data_projects.jl")

"""
In analogy to `Base.LOAD_PATH` and `Base.DEPOT_PATH`,

 - `@` means the path of the current active project as returned by
   `Base.active_project(false)` This can be useful when you're "doing
   scripting" and you've got a project-specific Data.toml which resides
   next to the Project.toml. This only applies to projects which are explicitly
   set with `julia --project` or `Pkg.activate()`.
 - Explicit paths may be either directories or files in Data.toml format.
   For directories, the filename "Data.toml" is implicitly appended.
 - As in DEPOT_PATH, an *empty* path component means the user's default
   Julia home directory, `joinpath(homedir(), ".julia", "datasets")`

This simplified version of the code loading rules (LOAD_PATH/DEPOT_PATH) is
used as it seems unlikely that we'll want data location to be version-
dependent in the same way that that code is.

Unlike `LOAD_PATH`, `JULIA_DATASETS_PATH` is represented inside the program as
a stack of `AbstractDataProject`

TODO: Do we want the `@name` form for searching for a specific named
environment similar to DEPOT_PATH? How would these be found?
"""
function _init_data_project_stack!(project_stack, env)
    env_search_path = get(env, "JULIA_DATASETS_PATH", nothing)
    if isnothing(env_search_path) || env_search_path == ""
        paths = ["@", ""]
    else
        paths = expanduser.(split(env_search_path, Sys.iswindows() ? ';' : ':'))
    end
    for path in paths
        if path == "@"
            project = ActiveDataProject()
        else
            if path == ""
                path = joinpath(homedir(), "datasets", "Data.toml")
            else
                path = abspath(path)
                if isdir(path)
                    path = joinpath(path, "Data.toml")
                end
            end
            project = TomlFileDataProject(path)
        end
        push!(project_stack, project)
    end
end

function __init__()
    _init_data_project_stack!(DATA_PROJECTS, ENV)
end

#-------------------------------------------------------------------------------
# Global datasets configuration for current Julia session
#
# TODO: Deprecate this ?
_current_project = DataProject()

"""
    load_project!(path_or_config)

Like `load_project()`, but populates the default global dataset project.
"""
function load_project!(path_or_config)
    # TODO: Deprecate this ?
    global _current_project = load_project(path_or_config)
end

# A stack of data projects, with the top of the stack being searched first.
DATA_PROJECTS = AbstractDataProject[]

_data_projects() = AbstractDataProject[_current_project; DATA_PROJECTS]

dataset(name) = dataset(_data_projects(), name)

function dataset(data_projects::AbstractVector{<:AbstractDataProject}, name)
    for project in data_projects
        d = get(project, name, nothing)
        if !isnothing(d)
            return d
        end
    end
    error("Dataset named $name not found")
end

function dataset_names()
    names = []
    for project in _data_projects()
        append!(names, keys(project))
    end
    unique(names)
end

#-------------------------------------------------------------------------------
# Storage layer and interface

_drivers = Dict{String,Any}()

"""
    add_storage_driver(driver_name=>storage_opener)

Associate DataSet storage driver named `driver_name` with `storage_opener`.
When a `dataset` with `storage.driver == driver_name` is opened,
`storage_opener(user_func, storage_config, dataset)` will be called. Any
existing storage driver registered to `driver_name` will be overwritten.

As a matter of convention, `storage_opener` should generally take configuration
from `storage_config` which is just `dataset.storage`. But to avoid config
duplication it may also use the content of `dataset`, (for example, dataset.uuid).

Packages which define new storage drivers should generally call
`add_storage_driver()` within their `__init__()` functions.
"""
function add_storage_driver((name,opener)::Pair)
    _drivers[name] = opener
end

function Base.open(f::Function, as_type, dataset::DataSet)
    storage_config = dataset.storage
    driver = _drivers[storage_config["driver"]]
    driver(storage_config, dataset) do storage
        open(f, as_type, storage)
    end
end

# For convenience, this non-scoped open() just returns the data handle as
# opened. See check_scoped_open for a way to help users avoid errors when using
# this (ie, if `identity` is not a valid argument to open() because resources
# would be closed before it returns).
#
# FIXME: Consider removing this. It should likely be replaced with `load()`, in
# analogy to FileIO.jl's load operation:
# * `load()` is "load the entire file into memory as such-and-such type"
# * `open()` is "open this resource, and run some function while it's open"
Base.open(as_type, conf::DataSet) = open(identity, as_type, conf)

"""
    check_scoped_open(func, as_type)

Call `check_scoped_open(func, as_type) in your implementation of `open(func,
as_type, data)` if you clean up or `close()` resources by the time `open()`
returns.

That is, if the unscoped form `use(open(AsType, data))` is invalid and the
following scoped form required:

```
open(AsType, data) do x
    use(x)
end
```

The dicotomy of resource handling techniques in `open()` are due to an
unresolved language design problem of how resource handling and cleanup should
work (see https://github.com/JuliaLang/julia/issues/7721).
"""
check_scoped_open(func, as_type) = nothing

function check_scoped_open(func::typeof(identity), as_type)
    throw(ArgumentError("You must use the scoped form `open(your_function, AsType, data)` to open as type $as_type"))
end

# Application entry points
include("entrypoint.jl")

# Builtin Data models
include("BlobTree.jl")

# Builtin backends
include("filesystem.jl")

# Backends
# include("ZipTree.jl")
# include("GitTree.jl")

# Application-level stuff
# include("repl.jl")

end
