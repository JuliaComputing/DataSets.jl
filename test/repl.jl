using DataSets.DataREPL: complete, parse_data_repl_cmd

@testset "repl completions" begin
    @test complete("") == (["list ", "show ", "stack ", "help "], "", true)
    @test complete("s") == (["show ", "stack "], "s", true)
    @test complete("stack ") == (["push ", "pop ", "list "], "", true)

    cd(@__DIR__) do
        if Sys.iswindows()
            @test complete("stack push da") == (["data\\"], "da", true)
        else
            @test complete("stack push da") == (["data/"], "da", true)
        end
    end
end

@testset "repl commands" begin
    @test eval(parse_data_repl_cmd("help")) === DataSets.DataREPL._data_repl_help
    @test eval(parse_data_repl_cmd("?")) === DataSets.DataREPL._data_repl_help
    empty!(DataSets.PROJECT)
    @test eval(parse_data_repl_cmd("stack push $(@__DIR__)")) === DataSets.PROJECT
    @test length(DataSets.PROJECT.projects) == 1
    @test eval(parse_data_repl_cmd("stack pop")) === DataSets.PROJECT
    @test isempty(DataSets.PROJECT.projects)
end

@testset "data show utils" begin
    @test sprint(DataSets.DataREPL.hexdump, UInt8.(0:70)) == raw"""
    0000: 0001 0203 0405 0607 0809 0a0b 0c0d 0e0f  ................
    0010: 1011 1213 1415 1617 1819 1a1b 1c1d 1e1f  ................
    0020: 2021 2223 2425 2627 2829 2a2b 2c2d 2e2f   !"#$%&'()*+,-./
    0030: 3031 3233 3435 3637 3839 3a3b 3c3d 3e3f  0123456789:;<=>?
    0040: 4041 4243 4445 46                        @ABCDEF         
    """
end
