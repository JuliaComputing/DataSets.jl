ENV["JULIA_DATASETS_PATH"] = ""

using DataSets
using Test
using UUIDs
using ResourceContexts

using DataSets: FileSystemRoot

@testset "register_post_init_callback" begin
    init_was_called = Ref(false)
    DataSets.register_post_init_callback() do
        init_was_called[] = true
    end
    @test init_was_called[]
end

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

    blob_in_tree_data = dataset(proj, "a_tree_example#1.csv")
    @test open(blob_in_tree_data) isa Blob
    @context begin
        @test @!(open(String, blob_in_tree_data)) == """Name,Age\n"Aaron",23\n"Harry",42\n"""
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
    @testset "Valid name: $name" for name in (
        "a_b", "a-b", "a1", "δεδομένα", "a/b", "a/b/c", "a-", "b_",
        "1", "a/1", "123", "12ab/34cd", "1/2/3", "1-2-3", "x_-__", "a---",
    )
        @test DataSets.check_dataset_name(name) === nothing
        @test DataSets._split_dataspec(name) == (name, nothing, nothing)
    end

    @testset "Invalid name: $name" for name in (
        "a b", "a.b", "a/b/", "a//b", "/a/b", "a/-", "a/ _/b",
        "a/-a", "a/-1",
    )
        @test_throws ErrorException DataSets.check_dataset_name(name)
        @test DataSets._split_dataspec(name) == (nothing, nothing, nothing)
    end
end

@testset "URL-like dataspec parsing" begin
    # Valid dataspecs
    DataSets._split_dataspec("foo?x=1#f") == ("foo", ["x" => "1"], "f")
    DataSets._split_dataspec("foo#f") == ("foo", nothing, "f")
    DataSets._split_dataspec("foo?x=1") == ("foo", ["x" => "1"], nothing)
    DataSets._split_dataspec("foo?x=1") == ("foo", ["x" => "1"], nothing)
    # Invalid dataspecs
    DataSets._split_dataspec("foo ?x=1") == (nothing, nothing, nothing)
    DataSets._split_dataspec("foo\n?x=1") == (nothing, nothing, nothing)
    DataSets._split_dataspec("foo\nbar?x=1") == (nothing, nothing, nothing)
    DataSets._split_dataspec(" foo?x=1") == (nothing, nothing, nothing)
    DataSets._split_dataspec("1?x=1") == (nothing, nothing, nothing)
    DataSets._split_dataspec("foo-?x=1") == (nothing, nothing, nothing)
    DataSets._split_dataspec("foo #f") == (nothing, nothing, nothing)
    DataSets._split_dataspec("@?x=1") == (nothing, nothing, nothing)

    proj = DataSets.load_project("Data.toml")

    @test !haskey(dataset(proj, "a_text_file"), "dataspec")

    # URL-like query
    @test dataset(proj, "a_text_file?x=1&yy=2")["dataspec"]["query"] == Dict("x"=>"1", "yy"=>"2")
    @test dataset(proj, "a_text_file?y%20y=x%20x")["dataspec"]["query"] == Dict("y y"=>"x x")
    @test dataset(proj, "a_text_file?x=%3d&y=%26")["dataspec"]["query"] == Dict("x"=>"=", "y"=>"&")

    # URL-like fragment
    @test dataset(proj, "a_text_file#a/b")["dataspec"]["fragment"] == "a/b"
    @test dataset(proj, "a_text_file#x%20x")["dataspec"]["fragment"] == "x x"
    @test dataset(proj, "a_text_file#x%ce%b1x")["dataspec"]["fragment"] == "xαx"

    # Combined query and fragment
    @test dataset(proj, "a_text_file?x=1&yy=2#frag")["dataspec"]["query"] == Dict("x"=>"1", "yy"=>"2")
    @test dataset(proj, "a_text_file?x=1&yy=2#frag")["dataspec"]["fragment"] == "frag"
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
