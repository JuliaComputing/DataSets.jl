using Base: PkgId

struct DataLayer{Tag}
    parameters::Dict{String,Any}
end

mutable struct LayerInfo
    pkgid::PkgId
    mod::Union{Nothing,Module}
end

const _layer_registry_lock = ReentrantLock()
const _layer_registry = Dict{String,Any}()

function register_layer!(type::AbstractString, uuid::AbstractString,
                         pkgname::AbstractString,
                         mod::Union{Nothing,Module}=nothing)
    layer_info = LayerInfo(PkgId(UUID(uuid), pkgname), mod)
    lock(_layer_registry_lock) do
        push!(_layer_registry, type=>layer_info)
    end
end

function load_layer(config::AbstractDict)
    load_layer(config["type"], get(config, "parameters", Dict{String,Any}()))
end

function _require(pkgid::PkgId)
    if !Base.root_module_exists(pkgid)
        # The problem here is that the loader code may be running in an old
        # world age
        @warn """
              The package $pkgid is required to load your dataset. `DataSets`
              will import this module for you, but this may not always work
              as expected.

              To silence this message, add `import $(pkgid.name)` at the top of
              your code somewhere.
              """
    end
    Base.require(pkgid)
end

function load_layer(type, parameters)
    layer_info = nothing
    mod = lock(_layer_registry_lock) do
        layer_info = get(_layer_registry, type, nothing)
        if isnothing(layer_info)
            error("No data layer named \"$type\" found in layer registry")
        end
        if isnothing(layer_info.mod)
            try
                # This require()-based loading is basically like what FileIO.jl does.
                layer_info.mod = _require(layer_info.pkgid)
            catch
                pkgid = layer_info.pkgid
                error("""Package $pkgid is required in your Manifest to load data layer of type \"$type\"""")
                # TODO
                # Run `DataSets.install_dataset_packages(your_dataset))` to fix this.""")
            end
        end
        layer_info.mod
    end
    DataLayer{Symbol(type)}(parameters), mod
end

#=
# TODO: Helper
"""
    install_dataset_packages(dataset)

Use `Pkg` to install any source code packages required to load the data in
`dataset`. These packages are installed into the current project.
"""
function install_dataset_packages()
    lock(_layer_registry_lock) do
        layers_config = get(dataset.conf, "layers", nothing)
        if !isnothing(layers_config)
            for layer_config in layers_config
                type = layer_config["type"]
                layer_info = get(_layer_registry, type, nothing)
                if isnothing(layer_info)
                    error("No data layer named \"$type\" found in layer registry")
                end
            end
        end
    end
end
=#

# By default, defer to any module outside DataSets to implement open().
@! function Base.open(layer::DataLayer, mod::Module, data)
    @! open(layer, data)
end

#--------------------------------------------------
# Use Base.identify_package(pkgname) to get PkgId to feed in here

register_layer!("csv", "336ed68f-0bac-5ca0-87d4-7b16caf5d00b", "CSV")
@! function Base.open(layer::DataLayer{:csv}, CSV::Module, blob::Blob)
    @! open(layer, CSV, @! open(Vector{UInt8}, blob))
end
@! function Base.open(layer::DataLayer{:csv}, CSV::Module, io::IO)
    @! open(layer, CSV, read(io))
end
@! function Base.open(layer::DataLayer{:csv}, CSV::Module, buf::Vector{UInt8})
    # Example use of layer parameters
    delim = only(get(layer.parameters, "delim", ",")) # Must be a single Char
    CSV.File(buf; delim)
end

register_layer!("arrow", "69666777-d1a9-59fb-9406-91d4454c9d45", "Arrow")
@! function Base.open(layer::DataLayer{:arrow}, Arrow::Module, blob::Blob)
    buf = @! open(Vector{UInt8}, blob)
    Arrow.Table(buf)
end

register_layer!("gzip", "944b1d66-785c-5afd-91f1-9de20f533193", "CodecZlib")
@! function Base.open(layer::DataLayer{:gzip}, CodecZlib::Module, blob::Blob)
    io = @! open(IO, blob)
    CodecZlib.GzipDecompressorStream(io)
end
# TODO: CodecXz, CodecZstd, CodecBzip2, CodecLz4 ?

#=
register_layer!("zip", "a5390f91-8eb1-5f08-bee0-b1d1ffed6cea", "ZipFile")
@! function Base.open(layer::DataLayer{:zip}, ZipFile::Module, blob::Blob)
    io = @! open(IO, blob)
    # TODO: Would be _much_ better if this presented with the BlobTree API.
    ZipFile.Reader(io)
end
=#

#--------------------------------------------------
# User-facing data APIs
#
# It may not always be desired for data owners to add these as a layer -
# how do we expose the underlying data source?

register_layer!("dataframe", "a93c6f00-e57d-5684-b7b6-d8193f3e46c0", "DataFrames")
@! function Base.open(layer::DataLayer{:dataframe}, DataFrames::Module, datasource)
    DataFrames.DataFrame(datasource)
end

