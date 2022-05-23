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
        _check_keys(conf, DataSet, ["uuid"=>String, "storage"=>Dict, "name"=>String])
        _check_keys(conf["storage"], DataSet, ["driver"=>String])
        _check_optional_keys(conf,
                             "description"=>AbstractString,
                             "tags"=>VectorOf(AbstractString))
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

_key_match(config, (k,T)::Pair) = haskey(config, k) && config[k] isa T
_key_match(config, k::String) = haskey(config, k)

function _check_keys(config, context, keys)
    missed_keys = filter(k->!_key_match(config, k), keys)
    if !isempty(missed_keys)
        error("""
              Missing expected keys in $context:
              $missed_keys

              In DataSet fragment:
              $(sprint(TOML.print,config))
              """)
    end
end

struct VectorOf
    T
end

function _check_optional_keys(config, context, keys...)
    for (k, check) in keys
        if haskey(config, k)
            v = config[k] 
            if check isa Type && !(v isa check)
                error("""Invalid DataSet key $k. Expected type $check""")
            elseif check isa VectorOf && !(v isa AbstractVector &&
                                           all(x isa check.T for x in v))
                error("""Invalid DataSet key $k""")
            end
        end
    end
end

"""
    is_valid_dataset_name(name)

Check whether a dataset name is valid. Valid names include start with a letter
and may contain letters, numbers or `_`. Names may be hieracicial, with pieces
separated with forward slashes. Examples:

    my_data
    my_data_1
    username/data
    organization/project/data
"""
function is_valid_dataset_name(name::AbstractString)
    # DataSet names disallow most punctuation for now, as it may be needed as
    # delimiters in data-related syntax (eg, for the data REPL).
    dataset_name_pattern = r"
        ^
        [[:alpha:]]
        (?:
            [-[:alnum:]_]     |
            / (?=[[:alpha:]])
        )*
        $
        "x
    return occursin(dataset_name_pattern, name)
end

function make_valid_dataset_name(name)
    if !is_valid_dataset_name(name)
        name = replace(name, r"^[^[:alpha:]]+"=>"")
        name = replace(name, '\\'=>'/')
        name = replace(name, r"[^-[:alnum:]_/]"=>"_")
        if !is_valid_dataset_name(name)
            # best-effort fallback
            name = "data"
        end
    end
    return name
end

function check_dataset_name(name::AbstractString)
    if !is_valid_dataset_name(name)
        error("DataSet name \"$name\" is invalid. DataSet names must start with a letter and can contain only letters, numbers, `_` or `/`.")
    end
end

# Hacky thing until we figure out which fields DataSet should actually have.
function Base.getproperty(d::DataSet, name::Symbol)
    if name in fieldnames(DataSet)
        return getfield(d, name)
    else
        getfield(d, :conf)[string(name)]
    end
end

Base.getindex(d::DataSet, name::AbstractString) = getindex(d.conf, name)
Base.haskey(d::DataSet, name::AbstractString) = haskey(d.conf, name)

# Split the fragment section as a '/' separated RelPath
function dataspec_fragment_as_path(d::DataSet)
    if haskey(d, "dataspec")
        fragment = get(d.dataspec, "fragment", nothing)
        if !isnothing(fragment)
            return RelPath(split(fragment, '/'))
        end
    end
    return nothing
end

function Base.show(io::IO, d::DataSet)
    print(io, DataSet, "(name=$(repr(d.name)), uuid=$(repr(d.uuid)), #= … =#)")
end

function Base.show(io::IO, ::MIME"text/plain", d::DataSet)
    println(io, "DataSet instance:")
    println(io)
    TOML.print(io, d.conf)
end


#-------------------------------------------------------------------------------
# Functions for opening datasets

# do-block form of open()
function Base.open(f::Function, as_type, dataset::DataSet)
    storage_config = dataset.storage
    driver = _find_driver(dataset)
    driver(storage_config, dataset) do storage
        open(f, as_type, storage)
    end
end

# Contexts-based form of open()
@! function Base.open(dataset::DataSet)
    storage_config = dataset.storage
    driver = _find_driver(dataset)
    # Use `enter_do` because drivers don't yet use the ResourceContexts.jl mechanism
    (storage,) = @! enter_do(driver, storage_config, dataset)
    storage
end

@! function Base.open(as_type, dataset::DataSet)
    storage = @! open(dataset)
    @! open(as_type, storage)
end

# TODO:
#  Consider making a distinction between open() and load().

# Finalizer-based version of open()
function Base.open(dataset::DataSet)
    @context begin
        result = @! open(dataset)
        @! ResourceContexts.detach_context_cleanup(result)
    end
end

function Base.open(as_type, dataset::DataSet)
    @context begin
        result = @! open(as_type, dataset)
        @! ResourceContexts.detach_context_cleanup(result)
    end
end

