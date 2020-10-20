module DataSets

using UUIDs
using HTTP.URIs # TODO: Use standalone URI library!
using Pkg # For Pkg.TOML

# using CSV, CodecZlib
# using HDF5

export DataSet, dataset

#-------------------------------------------------------------------------------

struct DataLayer
    type::String
    parameters::Vector
end

function read_toml(::Type{DataLayer}, t)
    DataLayer(t["type"], collect(get(t, "parameters", [])))
end

# The underlying configuration held in a DataSet
struct DataConfig
    default_name::String # Default name for convenience.
                         # The binding to an actual name is managed by the data
                         # project.
    location       # Resource definition (URI?)
    uuid::UUID     # Unique ID for use in distributed settings
    layers::Vector{DataLayer}

    # Generic dictionary of other properties... for now. Required properties
    # will be moved
    _other::Dict{Symbol,Any}

    #storage_id     # unique identifier in storage backend, if it exists
    #owner          # Project or user who owns the data
    #storage        #
    #protocol       #
    #description::String
    #type           # Some representation of the type of data?
    #               # An array, blob, table, tree, etc
    #cachable::Bool # Can the data be cached?  It might not for data governance
    #               # reasons or it might change commonly.
    ## A set of identifiers
    #tags::Set{String}
end

function DataConfig(; default_name, location, uuid=uuid4(), layers, kws...)
    DataConfig(default_name, location, uuid, layers, Dict{Symbol,Any}(kws))
end

function read_toml(::Type{DataConfig}, t)
    layers = read_toml.(DataLayer, t["layers"])
    locstring = t["location"]
    location = URI(locstring)
    DataConfig(default_name = t["default_name"],
               location = location,
               uuid = UUID(t["uuid"]),
               layers = layers)
end

# Hacky thing until we figure out which fields DataConfig should actually have.
function Base.getproperty(d::DataConfig, name::Symbol)
    if name in fieldnames(DataConfig)
        return getfield(d, name)
    else
        getfield(d, :_other)[name]
    end
end

function Base.show(io::IO, d::DataConfig)
    print(io, DataConfig, " $(d.default_name) @ $(repr(d.location))")
end

function load_data_toml(filename)
    toml = Pkg.TOML.parse(read(filename, String))
    project = toml["dataproject"]
end

#-------------------------------------------------------------------------------
"""
A `DataSet` is a metadata overlay for data held locally or remotely which is
unopinionated about the underlying storage mechanism.

The data in a `DataSet` has a type which implies an index; the index can be
used to partition the data for processing.
"""
struct DataSet{Tag}
    config::DataConfig
end

function DataSet(config::DataConfig)
    type = last(config.layers).type
    DataSet{Symbol(type)}(config)
end

function Base.show(io::IO, d::DataSet)
    print(io, typeof(d), " $(d.config.default_name) @ $(repr(d.config.location))")
end

function Base.getproperty(d::DataSet, name::Symbol)
    c = getfield(d, :config)
    name === :config ? c : getproperty(c, name)
end

function read_toml(::Type{DataSet}, t)
    DataSet(read_toml(DataConfig, t))
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
    toml = Pkg.TOML.parse(read(filename, String))
    available_datasets = Dict(d.uuid=>d for d in read_toml.(DataSet, toml["datasets"]))
    proj = DataProject()
    for entry in toml["dataproject"]["datasets"]
        id = UUID(entry["uuid"])
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
        print(io, "  ", name, ' '^pad, " => ", data.location)
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

# Prototype stuff. Put this back in once plain files are working.
# include("ZipTree.jl")
# include("GitTree.jl")
# include("S3Tree.jl") ...

# Application-level stuff
include("repl.jl")

#-------------------------------------------------------------------------------

function Base.open(f::Function, ::Type{IO}, d::DataSet{:File})
    open(AbsPath, d) do d_path
        return open(f, d_path)
    end
end

function Base.open(f::Function, ::Type{AbsPath}, d::DataSet{:File})
    location = d.location
    if location.scheme == "file"
        path = location.path
    elseif location.scheme == "" # FIXME: relative to where ?
        path = location.path
    else
        error("Only file URI schemes are supported ($location)")
    end
    p = AbsPath(FileTreeRoot(dirname(path)), RelPath([basename(path)]))
    f(p)
end

function Base.open(f::Function, ::Type{String}, d::DataSet{:File})
    open(IO, d) do io
        f(read(io, String))
    end
end

end
