# Data entry point functions
read_data = nothing

@datafunc function main1(x::File=>String, t::FileTree=>FileTree)
    csv_data = open(IO, t["1.csv"]) do io
        read(io,String)
    end
    global read_data = (x_string=x, csv_data=csv_data)
end

@datafunc function main1(x::File=>IO)
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

