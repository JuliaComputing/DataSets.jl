using DataSets
using Test
using UUIDs

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

#-------------------------------------------------------------------------------
@testset "open() functions" begin
    blob = Blob(FileSystemRoot("data/file.txt"))
    @test        open(identity, String, blob)         == "Hello world!\n"
    @test String(open(identity, Vector{UInt8}, blob)) == "Hello world!\n"
    @test open(io->read(io,String), IO, blob)         == "Hello world!\n"
    @test open(io->read(io,String), IO, blob)         == "Hello world!\n"
    @test open(identity, Blob, blob) === blob
    # Unscoped form for types which support it.
    @test open(String, blob)                == "Hello world!\n"
    @test String(open(Vector{UInt8}, blob)) == "Hello world!\n"
    @test_throws ArgumentError("You must use the scoped form `open(your_function, AsType, data)` to open as type IO") open(IO, blob)

    tree = BlobTree(FileSystemRoot("data"))
    @test open(identity, BlobTree, tree) === tree
end

#-------------------------------------------------------------------------------
# Data entry points
read_data = nothing

@datafunc function main1(x::Blob=>String, t::BlobTree=>BlobTree)
    csv_data = open(IO, t["1.csv"]) do io
        read(io,String)
    end
    global read_data = (x_string=x, csv_data=csv_data)
end

@datafunc function main1(x::Blob=>IO)
    x_data = read(x, String)
    global read_data = (x_data=x_data,)
end


@testset "@datafunc and @datarun" begin
    proj = DataSets.load_project("Data.toml")

    @datarun proj main1("a_text_file", "a_tree_example")

    @test read_data == (x_string="Hello world!\n",
                        csv_data="Name,Age\n\"Aaron\",23\n\"Harry\",42\n")

    @datarun proj main1("a_text_file")
    @test read_data == (x_data="Hello world!\n",)

    # No match for a single tree
    @test_throws ArgumentError @datarun proj main1("a_tree_example")
end

@testset "@datarun with DataSet.PROJECT" begin
    empty!(DataSets.PROJECT)
    DataSets.load_project!("Data.toml")

    @test dataset("a_text_file").uuid == UUID("b498f769-a7f6-4f67-8d74-40b770398f26")

    global read_data = nothing
    @datarun main1("a_text_file")
    @test read_data == (x_data="Hello world!\n",)
end

#-------------------------------------------------------------------------------
@testset "Data set parsing" begin
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
