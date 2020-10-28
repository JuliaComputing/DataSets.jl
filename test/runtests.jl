using DataSets
using Test

#--------------------------------------------------

read_data = nothing

@datafunc function main1(x::Blob=>String, t::Tree=>FileTree)
    csv_data = open(io->read(io,String), t["1.csv"])
    global read_data = (x_string=x, csv_data=csv_data)
end

@datafunc function main1(x::Blob=>IO)
    x_data = read(x, String)
    global read_data = (x_data=x_data,)
end

proj = DataSets.load_project("Data.toml")

@testset "@datafunc and @datarun" begin
    @datarun proj main1("a_text_file", "a_tree_example")

    @test read_data == (x_string="Some text file!\n",
                        csv_data="Name,Age\n\"Aaron\",23\n\"Harry\",42\n")

    @datarun proj main1("a_text_file")
    @test read_data == (x_data="Some text file!\n",)
end
