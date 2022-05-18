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
                "type"=>"File",
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
    @test open(text_data) isa File
    @test read(open(text_data), String) == "Hello world!\n"
    @context begin
        @test read(@!(open(text_data)), String) == "Hello world!\n"
    end

    tree_data = dataset(proj, "a_tree_example")
    @test open(tree_data) isa FileTree
    @context begin
        @test @!(open(tree_data)) isa FileTree
        tree = @! open(tree_data)
        @test readdir(tree) == ["1.csv", "2.csv"]
    end

    blob_in_tree_data = dataset(proj, "a_tree_example#1.csv")
    @test open(blob_in_tree_data) isa File
    @context begin
        @test @!(open(String, blob_in_tree_data)) == """Name,Age\n"Aaron",23\n"Harry",42\n"""
    end
end

#-------------------------------------------------------------------------------
@testset "from_path" begin
    file_dataset = DataSets.from_path(joinpath(@__DIR__, "data", "file.txt"))
    @test read(open(file_dataset), String) == "Hello world!\n"

    dir_dataset = DataSets.from_path(joinpath(@__DIR__, "data", "csvset"))

    @test open(dir_dataset) isa FileTree
    @test keys(open(dir_dataset)) == ["1.csv", "2.csv"]
end

#-------------------------------------------------------------------------------
@testset "Data set names" begin
    # Valid names
    @test DataSets.is_valid_dataset_name("a_b")
    @test DataSets.is_valid_dataset_name("a-b")
    @test DataSets.is_valid_dataset_name("a1")
    @test DataSets.is_valid_dataset_name("δεδομένα")
    @test DataSets.is_valid_dataset_name("a/b")
    @test DataSets.is_valid_dataset_name("a/b/c")
    # Invalid names
    @test !DataSets.is_valid_dataset_name("1")
    @test !DataSets.is_valid_dataset_name("a b")
    @test !DataSets.is_valid_dataset_name("a.b")
    @test !DataSets.is_valid_dataset_name("a/b/")
    @test !DataSets.is_valid_dataset_name("a//b")
    @test !DataSets.is_valid_dataset_name("/a/b")
    # Error message for invalid names
    @test_throws ErrorException("DataSet name \"a?b\" is invalid. DataSet names must start with a letter and can contain only letters, numbers, `_` or `/`.") DataSets.check_dataset_name("a?b")

    # Making valid names from path-like things
    @test DataSets.make_valid_dataset_name("a/b") == "a/b"
    @test DataSets.make_valid_dataset_name("a1") == "a1"
    @test DataSets.make_valid_dataset_name("1a") == "a"
    @test DataSets.make_valid_dataset_name("//a/b") == "a/b"
    @test DataSets.make_valid_dataset_name("a..b") == "a__b"
    @test DataSets.make_valid_dataset_name("C:\\a\\b") == "C_/a/b"
    # fallback
    @test DataSets.make_valid_dataset_name("a//b") == "data"
end

@testset "URL-like dataspec parsing" begin
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

include("FileTree.jl")
include("projects.jl")
include("entrypoint.jl")
include("repl.jl")
include("TomlDataStorage.jl")
include("backend_compat.jl")
include("driver_autoload.jl")
