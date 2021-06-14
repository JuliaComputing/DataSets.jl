ENV["JULIA_DATASETS_PATH"] = ""

using DataSets
using Test
using UUIDs
using ResourceContexts

using DataSets: FileSystemRoot

#-------------------------------------------------------------------------------
@testset "DataSet config" begin
    proj = DataSets.load_project("Data.toml")

    ds = dataset(proj, "a_text_file")
    @test ds.uuid == UUID("b498f769-a7f6-4f67-8d74-40b770398f26")
    @test ds.name == "a_text_file"
    @test ds.description == "A text file"
    @test ds.storage["driver"] == "FileSystem"
end

@testset "DataSet config from Dict" begin
    config = Dict(
        "data_config_version"=>0,
        "datasets"=>[Dict(
            "description"=>"A text file",
            "name"=>"a_text_file",
            "uuid"=>"b498f769-a7f6-4f67-8d74-40b770398f26",

            "storage"=>Dict(
                "driver"=>"FileSystem",
                "type"=>"Blob",
                "path"=>joinpath(@__DIR__, "data", "file.txt")
               )
           )]
       )

    proj = DataSets.load_project(config)

    ds = dataset(proj, "a_text_file")
    @test ds.uuid == UUID("b498f769-a7f6-4f67-8d74-40b770398f26")
end

@testset "open() for DataSet" begin
    proj = DataSets.load_project("Data.toml")

    text_data = dataset(proj, "a_text_file")
    @test open(text_data) isa Blob
    @test read(open(text_data), String) == "Hello world!\n"
    @context begin
        @test read(@!(open(text_data)), String) == "Hello world!\n"
    end

    tree_data = dataset(proj, "a_tree_example")
    @test open(tree_data) isa BlobTree
    @context begin
        @test @!(open(tree_data)) isa BlobTree
        tree = @! open(tree_data)
        @test readdir(tree) == ["1.csv", "2.csv"]
    end
end

#-------------------------------------------------------------------------------
@testset "open() for Blob and BlobTree" begin
    blob = Blob(FileSystemRoot("data/file.txt"))
    @test        open(identity, String, blob)         == "Hello world!\n"
    @test String(open(identity, Vector{UInt8}, blob)) == "Hello world!\n"
    @test open(io->read(io,String), IO, blob)         == "Hello world!\n"
    @test open(identity, Blob, blob) === blob
    # Unscoped forms
    @test open(String, blob)                == "Hello world!\n"
    @test String(open(Vector{UInt8}, blob)) == "Hello world!\n"
    @test read(open(IO, blob), String)      == "Hello world!\n"

    tree = BlobTree(FileSystemRoot("data"))
    @test open(identity, BlobTree, tree) === tree

    # Context-based forms
    @context begin
        @test @!(open(String, blob))               == "Hello world!\n"
        @test String(@! open(Vector{UInt8}, blob)) == "Hello world!\n"
        @test read(@!(open(IO, blob)), String)     == "Hello world!\n"
        @test @!(open(Blob, blob))                 === blob
        @test @!(open(BlobTree, tree))             === tree
    end
end

#-------------------------------------------------------------------------------
@testset "Data set name parsing" begin
    # Valid names
    @test DataSets.check_dataset_name("a_b") === nothing
    @test DataSets.check_dataset_name("a1") === nothing
    @test DataSets.check_dataset_name("δεδομένα") === nothing
    # Invalid names
    @test_throws ErrorException("DataSet name must start with a letter, and can only contain letters, numbers or underscores; got \"a/b\"") DataSets.check_dataset_name("a/b")
    @test_throws ErrorException DataSets.check_dataset_name("1")
    @test_throws ErrorException DataSets.check_dataset_name("a b")
    @test_throws ErrorException DataSets.check_dataset_name("a.b")
    @test_throws ErrorException DataSets.check_dataset_name("a:b")
end

#-------------------------------------------------------------------------------
# Trees
@testset "Temporary trees" begin
    function write_dir(j)
        d = newdir()
        for i=1:2
            d["hi_$i.txt"] = newfile() do io
                println(io, "hi $j $i")
            end
        end
        return d
    end

    temptree = newdir()
    for j=1:3
        temptree["d$j"] = write_dir(j)
    end
    @test open(io->read(io,String), IO, temptree["d1"]["hi_2.txt"]) == "hi 1 2\n"
    @test open(io->read(io,String), IO, temptree["d3"]["hi_1.txt"]) == "hi 3 1\n"
    @test isfile(DataSets.sys_abspath(temptree["d1"]["hi_2.txt"]))
end

include("projects.jl")
include("entrypoint.jl")
include("repl.jl")
include("DataTomlStorage.jl")
include("backend_compat.jl")
include("driver_autoload.jl")
