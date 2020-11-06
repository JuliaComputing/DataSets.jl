module DataSets

using UUIDs
using Pkg.TOML

# using CSV, CodecZlib
# using HDF5

export DataSet, dataset, @datafunc, @datarun
export FileTree, newfile, newdir

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
    conf

    function DataSet(conf)
        _check_keys(conf, DataSet, ["uuid", "storage", "name"])
        check_dataset_name(conf["name"])
        new(conf)
    end

    #=
    name::String # Default name for convenience.
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
    if !occursin(r"^[[:alnum:]_ ]*$", name)
        error("DataSet name is only allowed to contain letters, numbers, spaces or underscores; got \"$name\"")
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
    print(io, DataSet, " $(d.name) $(repr(d.uuid))")
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
    format_ver = toml["data_toml_version"]
    if format_ver > 0
        error("Data toml format version $format_ver is newer than supported")
    end
    proj = DataProject()
    for data_toml in toml["datasets"]
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
# Storage layer and interface

_drivers = Dict{String,Any}()

function Base.open(f::Function, as_type, conf::DataSet)
    storage_config = conf.storage
    driver = _drivers[storage_config["driver"]]
    driver(storage_config) do storage
        open(f, as_type, storage)
    end
end

include("filesystem.jl")

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

#-------------------------------------------------------------------------------
# Entry point utilities
#
# These make it easy for users to open `DataSet`s and map them into types
# understood by their program.

function extract_dtypes(call)
    dtypes = []
    jtypes = []
    argnames = []
    for ex in call.args[2:end]
        @assert ex.head == :call && ex.args[1] == :(=>)
        @assert ex.args[2].head == :(::) && length(ex.args[2].args) == 2
        push!(argnames, ex.args[2].args[1])
        push!(dtypes, ex.args[2].args[2])
        push!(jtypes, ex.args[3])
    end
    argnames, dtypes, jtypes
end

"""
    @datafunc function f(x::DT=>T, y::DS=>S...)
        ...
    end

Define the function `f(x::T, y::S, ...)` and add data dispatch rules so that
`f(x::DataSet, y::DataSet)` will open datasets matching dataset types `DT,DS`
as Julia types `T,S`.
"""
macro datafunc(func_expr)
    @assert func_expr.head == :function
    call = func_expr.args[1]
    body = func_expr.args[2]
    funcname = call.args[1]
    argnames, dtypes, jtypes = extract_dtypes(call)
    real_args = [:($n::$t) for (n,t) in zip(argnames, jtypes)]
    table_name = Symbol("#_$(funcname)_datasets_dispatch")
    esc_funcname = esc(funcname)
    esc_table_name = esc(table_name)
    func_expr.args[1].args[2:end] = real_args
    quote
        if !$(esc(:(@isdefined($table_name))))
            function $esc_funcname(ds::DataSet...)
                _run($esc_funcname, $esc_table_name, ds...)
            end
            const $esc_table_name = Dict()
        end
        push!($esc_table_name, tuple($(map(string, dtypes)...)) =>
                               tuple($(map(esc, jtypes)...)))
        $(esc(func_expr))
    end
end

function datarun(proj::DataProject, func::Function, data_names::AbstractString...)
    ds = map(n->dataset(proj, n), data_names)
    func(ds...)
end

"""
    @datarun [proj] func(args...)

Run `func` with the named `DataSet`s from the list `args`.

# Example

Load `DataSet`s named a,b as defined in Data.toml, and pass them to `f()`.
```
proj = DataSets.load_project("Data.toml")
@datarun proj f("a", "b")
```
"""
macro datarun(proj, call)
    esc_funcname = esc(call.args[1])
    esc_funcargs = esc.(call.args[2:end])
    quote
        datarun($(esc(proj)), $esc_funcname, $(esc_funcargs...))
    end
end

"""
    dataset_type(dataset)

Get a string representation of the "DataSet type", which represents the type of
the data *outside* Julia.

A given DataSet type may be mapped into many different Julia types. For example
consider the "Blob" type which is an array of bytes (commonly held in a file).
When loaded into Julia, this may be represented as a
    * IO             — via open())
    * String         — via open() |> read(_,String)
    * Vector{UInt8}  — via mmap)
    * Path
"""
function dataset_type(d::DataSet)
    # TODO: Enhance this once maps can be applied on top of the storage layer
    # Should we use MIME type? What about layering?
    d.storage["type"]
end

function _openall(func, opened, (dataset,T), to_open...)
    open(T, dataset) do newly_opened
        _openall(func, (opened..., newly_opened), to_open...)
    end
end

function _openall(func, opened)
    func(opened...)
end

# Match `dataset_type` of `ds` against `dispatch_table`, using the match to
# determine the appropriate Julia types we will open.
function _run(func, dispatch_table, ds::DataSet...)
    # For now, uses a simplistic exact matching strategy. We don't use Julia's
    # builtin dispatch here because
    # a) It seems wasteful to create a pile of tag types just for the purposes
    #    of matching some strings
    # b) It seems like a good idea to separate the declarative "data
    #    typesystem" (implicitly defined outside Julia) from Julia's type
    #    system and dispatch rules.
    dtypes = dataset_type.(ds)
    if !haskey(dispatch_table, dtypes)
        table = join(string.(first.(dispatch_table)), "\n")
        throw(ArgumentError("""No matching function $func for DataSet types $dtypes. Table:
                            $table
                            """))
    end
    julia_types = dispatch_table[dtypes]
    to_open = Pair.(ds, julia_types)
    _openall(func, (), to_open...)
end

end
