using DataSets
using Test

#-------------------------------------------------------------------------------
# Data entry points
read_data = nothing

@datafunc function main1(x::Blob=>String, t::Tree=>FileTree)
    csv_data = open(IO, t["1.csv"]) do io
        read(io,String)
    end
    global read_data = (x_string=x, csv_data=csv_data)
end

@datafunc function main1(x::Blob=>IO)
    x_data = read(x, String)
    global read_data = (x_data=x_data,)
end

proj = DataSets.load_project("Data.toml")

@testset "@datafunc and @datarun" begin
    @datarun proj main1("a_text_file", "a_tree_example")

    @test read_data == (x_string="Hello world!\n",
                        csv_data="Name,Age\n\"Aaron\",23\n\"Harry\",42\n")

    @datarun proj main1("a_text_file")
    @test read_data == (x_data="Hello world!\n",)
end

#-------------------------------------------------------------------------------
@testset "Data set parsing" begin
    @test DataSets.check_dataset_name("a_b") === nothing
    @test DataSets.check_dataset_name("a b") === nothing
    @test DataSets.check_dataset_name("δεδομένα") === nothing
    @test_throws ErrorException("DataSet name is only allowed to contain letters, numbers, spaces or underscores; got \"a/b\"") DataSets.check_dataset_name("a/b")
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
    @test isfile(DataSets._abspath(temptree["d1"]["hi_2.txt"]))
end
