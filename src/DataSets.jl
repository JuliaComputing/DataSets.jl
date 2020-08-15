module DataSets

using ReplMaker
#using SQLite
#using DBInterface
using ProtoStructs

export @path_str

# using FilePathsBase

# High level design:
#
#
# * Metadata store - a (distributed) registry of dataset metadata. This allows
#   us to overlay diverse data storage mechanisms with enough metadata to
#   ensure we can connect to them at an appropriate resource location and with
#   the appropriate protocol. Also to understand data governance (eg, can the
#   dataset be cached) etc etc.
#
#   This is (might be?) declarative configuration only; just enough to combine
#   a data storage backend + data connector.
#
# * Data storage backends - where the actual data is stored. These handle
#   versioning and persistence. More than one storage backend may hold a given
#   dataset.
#
# * Data REPL - this is the way that the user interacts with the metadata
#   store, and manages moving of data between different data storage backends.
#   (Eg, pushing from local storage to JuliaHub)
#
# * Data->compute connectors - this is the code components needed to get data
#   (local/remote) to the compute (local/remote)

# Metadata can produce the connector:
#
# get_connector(metadata) -> (storage, connector)

#= 
"""
A `DataSet` is a metadata overlay for data held locally or remotely which is
unopinionated about the underlying storage mechanism.

The data in a `DataSet` has a type which implies an index; the index can be
used to partition the data for processing.
"""
struct DataSet
    uuid::UUID     # Random unique ID
    location       # Filesystem path, URL or other resource location
    name::String   # Name, used as a default
    kws
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
=#

@proto DataSet
    # location
    # type
    # prototype

#=
function DataSet(uuid, location, name; kwargs...)
    DataSet(uuid, location, name, values(kwargs))
end
=#

function Base.show(io::IO, d::DataSet)
    println(io, DataSet, " $(d.type) @ $(repr(d.location))")
end

"""
    Used to close a dataset when an error has occurred.
"""
function abandon
end

"""

parents - parent datasets used in creating this one.
"""
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

# _open_methods = Dict(IO=>...)

function Base.open(d::DataSet, args...) #; parents=nothing)
    type = d.type
    # The following types refer to *data models*. This may be different from
    # the underlying storage... I think.
    #
    # There's two layers here:
    #   Data model - a *logical* model of the data
    #   Data access - Location and technical access means. Can this can be
    #                 encoded in the path type?
    if type == "File"
        # Data model for "File" is really a Blob: a plain sequence of bytes,
        # indexed by the offset.
        #
        # However, is it opened as a stream?
        open(d.location, args...)
    elseif type == "Vector{UInt8}"
        Mmap.mmap(d.location)
        # mmap the file?
    elseif type == "FileTree"
    elseif type == "Table"
    #elseif type == "GitFile"
    else
        error("Unrecognized type $(type)")
    end
end

# The code wants to have a table.
#
# The deployment environment says it's s3://b/data.zip * path/b.csv

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
function open(f::Function, d::DataSet, ::Type{IO}, read)
    open(f, d.location, read=read, write=!read)
end

function abandon()
end

function commit()
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

#=
# Possible "More Julian" form allowing small data to be returned as values
# and bound back to the dataset
@datafunc function bar(x::IO)::IO
end
=#

# S3 -> unzip -> traverse tree -> csv decode -> table

# Data models (???)

# Blob
#@proto File

# Tree-of-blobs
#@proto Directory

#=
{
    "type":"file",
    "relpath":"file.txt",
    "basepath": {
        "type":"git",
        "location":"/home/chris/.julia/dev/DataSets/scratch/blah"
    }
}
=#

# Interface

#=
# DataProject
# name -> uuid binding for current project
struct DataProject
    datasets::Vector{Pair{String,UUID}}
end
=#


# Data storage
#abstract type AbstractDataStorage end

include("FileTree.jl")

include("repl.jl")

end
