module DummyStorageBackends

using DataSets

struct DummyBackend
    data
end

function Base.open(f::Function, ::Type{IO}, storage::DummyBackend, path; kws...) where {T}
    @assert isempty(path)
    f(IOBuffer(storage.data))
end

function connect_dummy_backend(f, config, ds)
    storage = DummyBackend(config["data"])
    f(File(storage))
end

function __init__()
    DataSets.add_storage_driver("DummyTomlStorage"=>connect_dummy_backend)
end

end
