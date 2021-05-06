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
    print(io, DataSet, "(name=$(repr(d.name)), uuid=$(repr(d.uuid)), #= … =#)")
end

function Base.show(io::IO, ::MIME"text/plain", d::DataSet)
    TOML.print(io, d.conf)
end


#-------------------------------------------------------------------------------
"""
Subtypes of `AbstractDataProject` have the interface

Must implement:
  - `Base.get(project, dataset_name, default)` — search
  - `Base.keys(project)` - get dataset names

Optional:
  - `Base.iterate()`  — default implementation in terms of `keys` and `get`
  - `Base.pairs()`    — default implementation in terms of `keys` and `get`
  - `Base.haskey()`   — default implementation in terms of `get`
  - `Base.getindex()` — default implementation in terms of `get`
  - `DataSets.project_name()` — returns `nothing` by default.

Provided by AbstractDataProject (should not be overridden):
  - `DataSets.dataset()` - implemented in terms of `get`
"""
abstract type AbstractDataProject end

function Base.getindex(proj::AbstractDataProject, name::AbstractString)
    data = get(proj, name, nothing)
    data != nothing || error("Dataset $(repr(name)) not found")
    data
end

function dataset(proj::AbstractDataProject, name::AbstractString)
    # Non-fancy search... for now :)
    # In the future, we can consider parsing `name` into a dataset prefix and a
    # data selector / resource section. Eg a path for BlobTree which gives us a
    # SubDataSet
    proj[name]
end

function Base.haskey(proj::AbstractDataProject, name::AbstractString)
    get(proj, name, nothing) !== nothing
end

function Base.iterate(project::AbstractDataProject, state=nothing)
    if isnothing(state)
        ks = keys(project)
        ks_itr = iterate(ks)
    else
        (ks, ks_state) = state
        ks_itr = iterate(ks, ks_state)
    end
    if isnothing(ks_itr)
        return nothing
    end
    (k, ks_state) = ks_itr
    val = get(project, k, nothing)
    if isnothing(val)
        # val could be `nothing` if entries in the project are updated
        # concurrently. (Eg, this might happen for data projects which are
        # backed by the filesystem.)
        return iterate(project, (ks, ks_state))
    end
    (val, (ks, ks_state))
end

function Base.pairs(proj::AbstractDataProject)
    ks = keys(proj)
    (k=>d for (k,d) in (k=>get(proj, k, nothing) for k in ks) if !isnothing(d))
end

"""
    project_name(data_project)

Return the name of the given `data_project`. Ideally this can be used to
uniquely identify the project when modifying the project stack in
`DataSets.PROJECT`. For projects which were generated from
`JULIA_DATASETS_PATH`, this will be the expanded path component.

Other types of projects will have to return something else. For example, remote
data projects may want to return a URI. For projects which have no obvious
identifier, `nothing` is returned.
"""
project_name(data_project::AbstractDataProject) = nothing

#-------------------------------------------------------------------------------
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
    load_project([path | config_dict])

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

function Base.show(io::IO, ::MIME"text/plain", project::AbstractDataProject)
    datasets = collect(pairs(project))
    if isempty(datasets)
        print(io, typeof(project), " (empty)")
        return
    end
    summary(io, project)
    println(io, ":")
    sorted = sort(datasets, by=first)
    maxwidth = maximum(textwidth.(first.(sorted)))
    for (i, (name, data)) in enumerate(sorted)
        pad = maxwidth - textwidth(name)
        print(io, "  ", name, ' '^pad, " => ", data.uuid)
        if i < length(sorted)
            println(io)
        end
    end
end

function Base.summary(io::IO, project::AbstractDataProject)
    print(io, typeof(project))
    name = project_name(project)
    if !isnothing(name)
        print(io, " [", name, "]")
    end
end

#-------------------------------------------------------------------------------
"""
Search stack of AbstractDataProjects, where projects in the stack are searched
from first to last.

Additional projects may be added or removed from the stack with `pushfirst!`,
`push!` and `empty!`.
"""
struct DataProjectStack <: AbstractDataProject
    projects::Vector
end

DataProjectStack() = DataProjectStack([])

function Base.keys(stack::DataProjectStack)
    names = []
    for project in stack.projects
        append!(names, keys(project))
    end
    unique(names)
end

function Base.get(stack::DataProjectStack, name::AbstractString, default)
    for project in stack.projects
        d = get(project, name, nothing)
        if !isnothing(d)
            return d
        end
    end
end

# API for manipulating the stack.
Base.push!(stack::DataProjectStack, project) = push!(stack.projects, project)
Base.pushfirst!(stack::DataProjectStack, project) = pushfirst!(stack.projects, project)
Base.empty!(stack::DataProjectStack) = empty!(stack.projects)

function Base.show(io::IO, mime::MIME"text/plain", stack::DataProjectStack)
    summary(io, stack)
    println(io, ":")
    for (i,project) in enumerate(stack.projects)
        # show(io, mime, project)
        # indent each project
        str = sprint(show, mime, project)
        print(io, join("  " .* split(str, "\n"), "\n"))
        i != length(stack.projects) && println(io)
    end
end

include("file_data_projects.jl")

function expand_project_path(path)
    if path == "@"
        return path
    elseif path == ""
        return joinpath(homedir(), ".julia", "datasets", "Data.toml")
    else
        path = abspath(expanduser(path))
        if isdir(path)
            path = joinpath(path, "Data.toml")
        end
    end
    path
end

function create_project_stack(env)
    stack = []
    env_search_path = get(env, "JULIA_DATASETS_PATH", nothing)
    if isnothing(env_search_path) || env_search_path == ""
        paths = ["@", ""]
    else
        paths = split(env_search_path, Sys.iswindows() ? ';' : ':')
    end
    for path in paths
        if path == "@"
            project = ActiveDataProject()
        else
            project = TomlFileDataProject(expand_project_path(path))
        end
        push!(stack, project)
    end
    DataProjectStack(stack)
end

#-------------------------------------------------------------------------------
# Global datasets configuration for current Julia session

# Global stack of data projects, with the top of the stack being searched
# first.
"""
`DataSets.PROJECT` defines a global data environment for the Julia process. At
initialization, this is created from the `JULIA_DATASETS_PATH` environment
variable, which is a list of paths separated by `:` (or `;` on windows).

In analogy to `Base.LOAD_PATH` and `Base.DEPOT_PATH`, the path components are
interpreted as follows:

 - `@` means the path of the current active project as returned by
   `Base.active_project(false)` This can be useful when you're "doing
   scripting" and you've got a project-specific Data.toml which resides
   next to the Project.toml. This only applies to projects which are explicitly
   set with `julia --project` or `Pkg.activate()`.
 - Explicit paths may be either directories or files in Data.toml format.
   For directories, the filename "Data.toml" is implicitly appended.
   `expanduser()` is used to expand the user's home directory.
 - As in `DEPOT_PATH`, an *empty* path component means the user's default
   Julia home directory, `joinpath(homedir(), ".julia", "datasets")`

This simplified version of the code loading rules (LOAD_PATH/DEPOT_PATH) is
used as it seems unlikely that we'll want data location to be version-
dependent in the same way that that code is.

Unlike `LOAD_PATH`, `JULIA_DATASETS_PATH` is represented inside the program as
a `DataProjectStack`, and users can add custom projects by defining their own
`AbstractDataProject` subtypes.

Additional projects may be added or removed from the stack with `pushfirst!`,
`push!` and `empty!`.
"""
PROJECT = DataProjectStack()

# deprecated.
_current_project = DataProjectStack()

function __init__()
    global PROJECT = create_project_stack(ENV)
end

"""
    load_project!(path_or_config)

Prepends to the default global dataset search stack.

May be renamed in a future release.
"""
function load_project!(path_or_config)
    new_project = load_project(path_or_config)
    pushfirst!(PROJECT, new_project)
    _current_project = new_project
end

dataset(name) = dataset(PROJECT, name)

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
