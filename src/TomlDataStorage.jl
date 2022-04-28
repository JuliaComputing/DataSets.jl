using Base64

"""
Storage driver which keeps the data embedded within the TOML file itself.
Useful for small amounts of self-contained data.

## Metadata spec

For Blob:
```
    [datasets.storage]
    driver="TomlDataStorage"
    type="Blob"
    data=\$(base64encode(data))
```

For BlobTree:
```
    [datasets.storage]
    driver="TomlDataStorage"
    type="BlobTree"

        [datasets.storage.data.\$(dirname1)]
        "\$(filename1)" = \$(base64encode(data1))
        "\$(filename2)" = \$(base64encode(data2))

        [datasets.storage.data.\$(dirname2)]
        ...
```
"""
struct TomlDataStorage
    dataset::DataSet
    data::Union{String,Dict{String,Any}}
end

# Get TOML data at `path`, returning nothing if not present
function _getpath(storage::TomlDataStorage, path::RelPath)
    x = storage.data
    for c in path.components
        x = get(x, c, nothing)
        !isnothing(x) || return nothing
    end
    x
end

#--------------------------------------------------
# Storage data interface for trees

Base.isdir(storage::TomlDataStorage, path::RelPath) = _getpath(storage, path) isa Dict
Base.isfile(storage::TomlDataStorage, path::RelPath) = _getpath(storage, path) isa String
Base.ispath(storage::TomlDataStorage, path::RelPath) = !isnothing(_getpath(storage, path))

Base.summary(io::IO, storage::TomlDataStorage) = print(io, "Data.toml")

function Base.readdir(storage::TomlDataStorage, path::RelPath)
    try
        tree = _getpath(storage, path)
        !isnothing(tree) || KeyError(path)
        sort!(collect(keys(tree::AbstractDict)))
    catch
        error("TOML storage requires trees to be as TOML dictionaries")
    end
end

#--------------------------------------------------
# Storage data interface for Blob

function Base.open(func::Function, as_type::Type{IO},
                   storage::TomlDataStorage, path; kws...)
    @context func(@! open(as_type, storage, path; kws...))
end

@! function Base.open(::Type{Vector{UInt8}}, storage::TomlDataStorage, path;
                      write=false, read=!write, kws...)
    if write
        error("Embedded data is read-only from within the DataSets interface")
    end
    try
        str = _getpath(storage, path)
        !isnothing(str) || KeyError(path)
        base64decode(str::AbstractString)
    catch
        error("TOML storage requires data to be as base64 encoded strings")
    end
end

@! function Base.open(::Type{IO}, storage::TomlDataStorage, path; kws...)
    buf = @! open(Vector{UInt8}, storage, path; kws...)
    IOBuffer(buf)
end


# TODO: The following should be factored out and implemented generically
function Base.read(storage::TomlDataStorage, path::RelPath, ::Type{T}) where {T}
    @context begin
        io = @! open(IO, storage, path)
        read(io, T)
    end
end

function Base.read(storage::TomlDataStorage, path::RelPath)
    @context @! open(Vector{UInt8}, storage, path)
end


#-------------------------------------------------------------------------------
# Connect storage backend
function connect_toml_data_storage(f, config, dataset)
    type = config["type"]
    data = get(config, "data", nothing)
    if type == "Blob"
        if !(data isa AbstractString)
            error("TOML data storage requires string data in the \"storage.data\" key")
        end
        f(Blob(TomlDataStorage(dataset, data)))
    elseif type == "BlobTree"
        if !(data isa AbstractDict)
            error("TOML data storage requires a dictionary in the \"storage.data\" key")
        end
        f(BlobTree(TomlDataStorage(dataset, data)))
    else
        throw(ArgumentError("DataSet type $type not supported for data embedded in Data.toml"))
    end
end

add_storage_driver("TomlDataStorage"=>connect_toml_data_storage)

