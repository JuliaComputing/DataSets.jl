module DataSets

using UUIDs
using Pkg.TOML

# using CSV, CodecZlib
# using HDF5

export DataSet, dataset

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
    conf
    #=
    default_name::String # Default name for convenience.
                         # The binding to an actual name is managed by the data
                         # project.
    uuid::UUID     # Unique ID for use in distributed settings
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

function read_toml(::Type{DataSet}, toml)
    @assert haskey(toml, "uuid") &&
            haskey(toml, "storage") &&
            haskey(toml, "default_name")
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
    print(io, DataSet, " $(d.default_name) @ $(repr(d.location))")
end

function Base.show(io::IO, ::MIME"text/plain", d::DataSet)
    TOML.print(io, d.conf)
end


#-------------------------------------------------------------------------------
"""
    DataProject

A data project is a collection of DataSets with associated names. Names are
unique within the project.
"""
struct DataProject
    datasets::Dict{String,DataSet}
end

DataProject() = DataProject(Dict{String,DataSet}())

function load_project(filename::AbstractString)
    toml_str = read(filename, String)
    # Super hacky templating for paths relative to the toml file.
    toml_str = replace(toml_str, "@__DIR__"=>dirname(abspath(filename)))
    toml = TOML.parse(toml_str)
    available_datasets = Dict(d.uuid=>d for d in read_toml.(DataSet, toml["datasets"]))
    proj = DataProject()
    for entry in toml["dataproject"]["datasets"]
        id = entry["uuid"]
        link_dataset(proj, entry["name"] => available_datasets[id])
    end
    proj
end

function link_dataset(proj::DataProject, (name,data)::Pair)
    proj.datasets[name] = data
end

link_dataset(proj::DataProject, d::DataSet) = link_dataset(proj, d.default_name=>d)

function unlink_dataset(proj::DataProject, name::AbstractString)
    if !haskey(proj.datasets, name)
        throw(ArgumentError("No dataset \"$name\" in data project"))
    end
    d = proj.datasets[name]
    delete!(proj.datasets, name)
    d
end

function dataset(proj::DataProject, name)
    proj.datasets[name]
end

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


#-------------------------------------------------------------------------------
# Dataset lifecycle

#=
function Base.open(f::Function, d::DataSet, args...) #; parents=nothing)
    data = open(d, args...) #; parents=parents)
    try
        f(data)
        # TODO: Distinguish close-with-success vs close-with-failure
        close(data) #, true)
    catch
        abandon(data) #close(data, false)
        rethrow()
    end
end
=#

#=
function Base.open(d::DataSet, args...)
    location = d.location
    if location.scheme == "file"
        path = location.path
    elseif location.scheme == ""
        path = location.path
    else
        error("Only file URI schemes are supported ($location)")
    end
    layers = d.layers
    if layers[1].type == "file"
        if length(layers) == 1
            return open(path, args...)
        #elseif layers[2] == "zip"
        #    ZippedFileTree(ZipTreeRoot(path, args...))
        # elseif layers[2] == "gzip"
        #     if length(layers) == 2
        #         GzipDecompressorStream(open(path))
        #     elseif layers[3] == "csv"
        #         # CSV.File(GzipDecompressorStream(open(path)))
        #     end
        end
    elseif layers[1].type == "tree"
        return FileTree(FileTreeRoot(path, args...))
    else
        error("Unrecognized type $(type)")
    end
end
=#

#=
macro datafunc(ex)
    if !Meta.isexpr(ex, :function)
        throw(ArgumentError("Must pass a function to `@datafunc`. Got `$ex`"))
    end
    callex = ex.args[1]
    @assert Meta.isexpr(callex, :call) # TODO allow other forms
    nargs = length(callex.args) - 1
    @assert narg == 0 || !Meta.isexpr(callex.args[1], :parameters) # TODO allow this
    funcname = callex.args[1]
    callargs = map(callex.args[2:end]) do arg
        if !Meta.isexpr(arg, :(::))
            throw(ArgumentError("Positional parameters must have types. Got `$arg`"))
        end
        arg.args
    end
    # Generate function override...
    # output_mask = endswith.(string.(callargs), "!")
end
=#

#=
@datafunc function foo(x::IO, y!::IO)
end
=#

# Generates something like

# For IO streams, dispatch to open with the location
function Base.open(f::Function, d::DataSet, ::Type{IO}, read)
    open(f, d.location, read=read, write=!read)
end

# Context-manager style scoping for opening arguments
function _open_data(f, opened_args)
    f(opened_args...)
end
function _open_data(f, opened_args, to_open, to_open_tail...)
    d, T, read = to_open
    # This first call to open() looks at `d` and runs code needed by the
    # storage model and format.
    open(d, read) do data_handle
        # This second call to open() attaches the model to the user's type
        open(data_handle, T) do d_T
            _open_data(f, (opened_args..., d_T), to_open_tail...)
        end
    end
end

function open_data(f, data_args)
    read_data = [d for d in data_args if d[3]]
    write_data = [d for d in data_args if !d[3]]
    # TODO: Metadata editing in the data backends.
    # write_data_handles = [start_commit(d, parents=read_data) for d in write_data]
    try
        _open_data(f, (), data_args...)
        # if d1 in write_data_handles
        #     finish_commit(d1)
        # end
    catch
        # if d1 in write_data_handles
        #     abandon_commit(d1)
        # end
        rethrow()
    end
end

#-------------------------------------------------------------------------------
# Built in Data models

include("paths.jl")
include("FileTree.jl")

# Prototype stuff. Put this back in once the core is working.
# include("ZipTree.jl")
# include("GitTree.jl")
# include("S3Tree.jl") ...
#
# Application-level stuff
# include("repl.jl")

#-------------------------------------------------------------------------------

"""
    connect(f, driver, config)

Connect to data storage driver to get a connection `conn` and run `f(conn)`.
"""
function connect
end

_drivers = Dict{String,Any}()

function Base.open(f::Function, as_type, conf::DataSet)
    storage_config = conf.storage
    driver = _drivers[storage_config["driver"]]
    connect(driver, storage_config) do storage
        open(f, as_type, storage)
    end
end

# For convenience, this somewhat dodgy function just returns the data handle as
# opened.
#
# If that's a data structure which is fully loaded into memory this is ok and
# super handy!
#
# But if not, the underlying data connection will have been closed by the time
# this function returns.  TODO: Have some trait or something which can
# determine whether this is safe.
Base.open(as_type, conf::DataSet) = open(identity, as_type, conf)

#--------------------------------------------------

struct FileSystemDriver
end

push!(_drivers, "FileSystem"=>FileSystemDriver())

struct FileSystemFile
    path::String
end

struct FileSystemDir
    path::String
end

function connect(f, driver::FileSystemDriver, config)
    path = config["path"]
    type = config["type"]
    if type == "Blob"
        isfile(path) || throw(ArgumentError("$(repr(path)) should be a file"))
        storage = FileSystemFile(path)
    elseif type == "Tree"
        isdir(path)  || throw(ArgumentError("$(repr(path)) should be a directory"))
        storage = FileSystemDir(path)
    end
    f(storage)
end

function Base.open(f::Function, ::Type{FileTree}, dir::FileSystemDir)
    f(FileTree(FileTreeRoot(dir.path)))
end

function Base.open(f::Function, ::Type{IO}, file::FileSystemFile)
    # TODO writeable files
    open(f, file.path; read=true, write=false)
end

function Base.open(f::Function, ::Type{String}, file::FileSystemFile)
    open(io->read(io,String), IO, file)
end


end
