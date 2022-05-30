using Base64

"""
Storage driver which keeps the data embedded within the TOML file itself.
Useful for small amounts of self-contained data.

## Metadata spec

For File:
```
    [datasets.storage]
    driver="TomlDataStorage"
    type="File"
    data=\$(base64encode(data))
```

For FileTree:
```
    [datasets.storage]
    driver="TomlDataStorage"
    type="FileTree"

        [datasets.storage.data.\$(dirname1)]
        "\$(filename1)" = \$(base64encode(data1))
        "\$(filename2)" = \$(base64encode(data2))

        [datasets.storage.data.\$(dirname2)]
        ...
```
"""
struct TomlDataRoot
    dataset::DataSet
    data::Union{Vector{UInt8},ConfigDict}
    write::Bool
end

_data_strings_to_buffers(data::String) = base64decode(data)
function _data_strings_to_buffers(data::Dict)
    ConfigDict(k=>_data_strings_to_buffers(v) for (k,v) in pairs(data))
end
_data_strings_to_buffers(data) = error("Unexpected embedded data: expected a string or dictionary")

_data_buffers_to_strings(data::Vector{UInt8}) = base64encode(data)
function _data_buffers_to_strings(data::Dict)
    ConfigDict(k=>_data_buffers_to_strings(v) for (k,v) in pairs(data))
end

# Get TOML data at `path`, returning nothing if not present
function _getpath(storage::TomlDataRoot, path::RelPath)
    x = storage.data
    for c in path.components
        if !(x isa AbstractDict)
            return nothing
        end
        x = get(x, c, nothing)
        if isnothing(x)
            return nothing
        end
    end
    x
end

#--------------------------------------------------
# Storage data interface for trees

Base.isdir(storage::TomlDataRoot, path::RelPath) = _getpath(storage, path) isa Dict
Base.isfile(storage::TomlDataRoot, path::RelPath) = _getpath(storage, path) isa String
Base.ispath(storage::TomlDataRoot, path::RelPath) = !isnothing(_getpath(storage, path))

Base.summary(io::IO, storage::TomlDataRoot) = print(io, "Data.toml")

function Base.readdir(storage::TomlDataRoot, path::RelPath)
    try
        tree = _getpath(storage, path)
        !isnothing(tree) || KeyError(path)
        sort!(collect(keys(tree::AbstractDict)))
    catch
        error("TOML storage requires trees to be as TOML dictionaries")
    end
end

#--------------------------------------------------
# Storage data interface for File

function Base.open(func::Function, as_type::Type{IO},
                   storage::TomlDataRoot, path; write=false, kws...)
    @context func(@! open(as_type, storage, path; write=false, kws...))
end

@! function Base.open(::Type{Vector{UInt8}}, storage::TomlDataRoot, path;
                      write=false)
    try
        buf = _getpath(storage, path)
        !isnothing(buf) || KeyError(path)
        return buf
    catch
        error("TOML storage requires data to be as base64 encoded strings")
    end
end

@! function Base.open(::Type{IO}, storage::TomlDataRoot, path; write=false)
    buf = @! open(Vector{UInt8}, storage, path; write=write)
    if write
        # For consistency with filesystem version of open()
        resize!(buf,0)
    end
    return IOBuffer(buf, write=write)
end

@! function Base.open(::Type{String}, storage::TomlDataRoot, path; write=false)
    buf = @! open(Vector{UInt8}, storage, path; write=write)
    return String(copy(buf))
end

function close_dataset(storage::TomlDataRoot, exc=nothing)
    if storage.write
        encoded_data = _data_buffers_to_strings(storage.data)
        # Force writing of dataset to project
        conf = copy(storage.dataset.storage)
        conf["data"] = encoded_data
        config(storage.dataset; storage=conf)
    end
end

# TODO: The following should be factored out and implemented generically
function Base.read(storage::TomlDataRoot, path::RelPath, ::Type{T}) where {T}
    @context begin
        io = @! open(IO, storage, path)
        read(io, T)
    end
end

function Base.read(storage::TomlDataRoot, path::RelPath)
    @context @! open(Vector{UInt8}, storage, path)
end


#-------------------------------------------------------------------------------
struct TomlDataDriver <: AbstractDataDriver
end

function open_dataset(driver::TomlDataDriver, dataset, write)
    type = dataset.storage["type"]
    data = get(dataset.storage, "data", nothing)
    if is_File_dtype(type)
        if !(data isa AbstractString)
            error("TOML data storage requires string data in the \"storage.data\" key")
        end
        return File(TomlDataRoot(dataset, _data_strings_to_buffers(data), write))
    elseif is_FileTree_dtype(type)
        if !(data isa AbstractDict)
            error("TOML data storage requires a dictionary in the \"storage.data\" key")
        end
        return FileTree(TomlDataRoot(dataset, _data_strings_to_buffers(data), write))
    else
        throw(ArgumentError("DataSet type $type not supported for data embedded in Data.toml"))
    end
end

