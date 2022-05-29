using Base64

# Compatibility for data backends which implement DataSets 0.2.0 storage
# interface.

#-------------------------------------------------------------------------------
struct OldBackendAPI
    data
end

function _lookup_path(storage::OldBackendAPI, path)
    # For backends which try to avoid depending on DataSets types
    path = string(path)
    isempty(path) ? storage.data : get(storage.data, path, nothing)
end

function _get_data(storage::OldBackendAPI, path)
    base64decode(_lookup_path(storage, path))
end

function Base.open(f::Function, ::Type{IO}, storage::OldBackendAPI, path; kws...) where {T}
    f(IOBuffer(_get_data(storage, path)))
end

function Base.read(storage::OldBackendAPI, path, ::Type{T}) where {T}
    read(IOBuffer(_get_data(storage, path)), T)
end

function Base.read(storage::OldBackendAPI, path)
    _get_data(storage, path)
end

function Base.readdir(storage::OldBackendAPI, path)
    if isempty(path)
        sort!(collect(keys(storage.data)))
    else
        []
    end
end

function Base.isdir(storage::OldBackendAPI, path)
    path = string(path)
    @assert storage.data isa Dict
    isempty(path)
end

function Base.isfile(storage::OldBackendAPI, path)
    _lookup_path(storage, path) isa String
end

function Base.ispath(storage::OldBackendAPI, path)
    !isnothing(_lookup_path(storage, path))
end


function connect_old_backend(f, config, ds)
    storage = OldBackendAPI(config["data"])
    if config["type"] == "Blob"
        f(Blob(storage))
    else
        f(BlobTree(storage))
    end
end

DataSets.add_storage_driver("OldBackendAPI"=>connect_old_backend)

#-------------------------------------------------------------------------------
@testset "OldBackendAPI" begin
    proj = DataSets.load_project(joinpath(@__DIR__, "DataCompat.toml"))

    @test open(IO, dataset(proj, "old_backend_blob")) do io
           read(io, String)
    end == "x"
    @test String(open(read, IO, dataset(proj, "old_backend_blob"))) == "x"
    @test open(Vector{UInt8}, dataset(proj, "old_backend_blob")) == UInt8['x']
    @test read(open(dataset(proj, "old_backend_blob")), String) == "x"
    @test read(open(dataset(proj, "old_backend_blob"))) == UInt8['x']

    @test readdir(open(dataset(proj, "old_backend_tree"))) == ["a.txt", "b.txt"]
    @test open(dataset(proj, "old_backend_tree"))[path"a.txt"] isa File
    @test read(open(dataset(proj, "old_backend_tree"))[path"a.txt"], String) == "a"
    @test read(open(dataset(proj, "old_backend_tree"))[path"b.txt"], String) == "b"
end

@testset "Compat for @__DIR__ and renaming Blob->File, BlobTree->FileTree" begin
    proj = DataSets.load_project(joinpath(@__DIR__, "DataCompat.toml"))

    text_data = dataset(proj, "a_text_file")
    @test open(text_data) isa Blob
    @test read(open(text_data), String) == "Hello world!\n"

    tree_data = dataset(proj, "a_tree_example")
    @context begin
        @test @!(open(tree_data)) isa BlobTree
        tree = @! open(tree_data)
        @test readdir(tree) == ["1.csv", "2.csv"]
    end
end
