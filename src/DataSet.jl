"""
A `DataSet` is a metadata overlay for data held locally or remotely which is
unopinionated about the underlying storage mechanism.

The data in a `DataSet` has a type which implies an index; the index can be
used to partition the data for processing.
"""
mutable struct DataSet
    project        # AbstractDataProject owning this DataSet
    uuid::UUID     # Unique identifier for the dataset. Use uuid4() to create these.
    # The representation `conf` contains "configuration data" read directly from
    # the TOML (or other data project source, eg json API etc)
    conf

    function DataSet(project, conf)
        _validate_dataset_config(conf)
        new(project, UUID(conf["uuid"]), conf)
    end
end

DataSet(conf) = DataSet(nothing, conf)

function _validate_dataset_config(conf)
    _check_keys(conf, DataSet, ["uuid"=>String, "storage"=>Dict, "name"=>String])
    _check_keys(conf["storage"], DataSet, ["driver"=>String])
    _check_optional_keys(conf,
                         "description"=>AbstractString,
                         "tags"=>VectorOf(AbstractString))
    check_dataset_name(conf["name"])
end

function Base.show(io::IO, d::DataSet)
    print(io, DataSet, "(name=$(repr(d.name)), uuid=$(repr(d.uuid)), #= … =#)")
end

function Base.show(io::IO, ::MIME"text/plain", d::DataSet)
    println(io, "DataSet instance:")
    println(io)
    TOML.print(io, d.conf)
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

#-------------------------------------------------------------------------------
# API for DataSet type
function Base.getproperty(d::DataSet, name::Symbol)
    if name === :uuid
        getfield(d, :uuid)
    elseif name === :conf
        getfield(d, :conf)
    else
        getfield(d, :conf)[string(name)]
    end
end

Base.getindex(d::DataSet, name::AbstractString) = getindex(d.conf, name)
Base.haskey(d::DataSet, name::AbstractString) = haskey(d.conf, name)

function data_project(dataset::DataSet)
    return getfield(dataset, :project)
end

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

function config!(dataset::DataSet; kws...)
    config!(data_project(dataset), dataset; kws...)
end

# The default case of a dataset config update when the update is independent of
# the project.  (In general, projects may supply extra constraints.)
function config!(::Nothing, dataset::DataSet; kws...)
    for (k,v) in pairs(kws)
        if k in (:uuid, :name)
            error("Cannot modify dataset config with key $k")
        # TODO: elseif k === :storage
            # Check consistency using storage driver API?
        end
        dataset.conf[string(k)] = v
    end
    return dataset
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

