# Tests for data project functionality

using Pkg

using DataSets:
    TomlFileDataProject,
    ActiveDataProject,
    StackedDataProject,
    project_name,
    config!

test_project_names = ["a_text_file",
                      "a_tree_example",
                      "embedded_blob",
                      "embedded_tree",
                      "gzipped_table",
                      "some_namespace/a_text_file"]

@testset "TomlFileDataProject" begin
    proj = TomlFileDataProject(abspath("Data.toml"))
    # getindex, get
    @test proj["a_text_file"].uuid == UUID("b498f769-a7f6-4f67-8d74-40b770398f26")
    @test_throws ErrorException proj["nonexistent_data"]
    @test isnothing(get(proj, "nonexistent_data", nothing))

    # keys
    @test sort(collect(keys(proj))) == test_project_names
    @test haskey(proj, "a_text_file")
    @test !haskey(proj, "nonexistent_data")

    # iteration
    @test sort(getproperty.(collect(proj), :name)) == test_project_names
    @test sort(first.(pairs(proj))) == test_project_names

    # identity
    @test project_name(proj) == abspath("Data.toml")
end

@testset "TomlFileDataProject live updates" begin
    # Test live updating when the file is rewritten
    mktemp() do path,io
        write(io, """
        data_config_version=1

        [[datasets]]
        description="A text file"
        name="a_text_file"
        uuid="b498f769-a7f6-4f67-8d74-40b770398f26"

            [datasets.storage]
            driver="FileSystem"
            type="File"
            path="data/file.txt"
        """)
        flush(io)

        proj = TomlFileDataProject(path)

        @test collect(keys(proj)) == ["a_text_file"]
        @test proj["a_text_file"].uuid == UUID("b498f769-a7f6-4f67-8d74-40b770398f26")

        write(io, """

        [[datasets]]
        description="A text file 2"
        name="a_text_file_2"
        uuid="58992dd5-86ed-4747-b9fc-320e13a03504"

            [datasets.storage]
            driver="FileSystem"
            type="File"
            path="data/file2.txt"
        """)
        flush(io)

        @test sort(collect(keys(proj))) == ["a_text_file", "a_text_file_2"]
        @test proj["a_text_file_2"].uuid == UUID("58992dd5-86ed-4747-b9fc-320e13a03504")
    end
end

@testset "ActiveDataProject" begin
    proj = ActiveDataProject()
    @test project_name(proj) == nothing
    @test collect(keys(proj)) == []
    @test collect(pairs(proj)) == []
    @test collect(proj) == []
    proj_dir = joinpath(@__DIR__, "active_project")
    Pkg.activate(proj_dir)
    @test project_name(proj) == joinpath(proj_dir, "Data.toml")
    @test collect(keys(proj)) == ["a_text_file"]
    @test proj["a_text_file"].uuid == UUID("314996ef-12be-40d0-912c-9755af354fdb")
    Pkg.activate()
    @test project_name(proj) == nothing
    @test collect(keys(proj)) == []
end

@testset "StackedDataProject" begin
    proj = StackedDataProject()
    @test collect(keys(proj)) == []

    push!(proj, TomlFileDataProject(joinpath(@__DIR__, "active_project", "Data.toml")))
    push!(proj, TomlFileDataProject(joinpath(@__DIR__, "Data.toml")))

    @test sort(collect(keys(proj))) == test_project_names
    # Data "a_text_file" should be found in the first project in the stack,
    # overriding the data of the same name in the second project.
    @test proj["a_text_file"].uuid == UUID("314996ef-12be-40d0-912c-9755af354fdb")
end

@testset "JULIA_DATASETS_PATH" begin
    # Test JULIA_DATASETS_PATH environment variable and path unpacking
    paths_sep = Sys.iswindows() ? ';' : ':'
    datasets_paths = join(["@",
                           joinpath(@__DIR__, "Data.toml"),
                           ""], paths_sep)
    fake_env = Dict("JULIA_DATASETS_PATH"=>datasets_paths)
    proj = DataSets.create_project_stack(fake_env)
    @test proj.projects[1] isa ActiveDataProject

    @test proj.projects[2] isa TomlFileDataProject
    @test project_name(proj.projects[2]) == joinpath(@__DIR__, "Data.toml")

    @test proj.projects[3] isa TomlFileDataProject
    @test project_name(proj.projects[3]) == joinpath(homedir(), ".julia", "datasets", "Data.toml")

    # Test that __init__ takes global DataSets.PROJECT from ENV
    empty!(DataSets.PROJECT)
    ENV["JULIA_DATASETS_PATH"] = @__DIR__
    DataSets.__init__()
    @test DataSets.PROJECT.projects[1] isa TomlFileDataProject
    @test project_name(DataSets.PROJECT.projects[1]) == joinpath(@__DIR__, "Data.toml")
end

@testset "config!() metadata update" begin
    # Test live updating when the file is rewritten
    mktempdir() do tmppath
        data_toml_path = joinpath(tmppath, "Data.toml")
        open(data_toml_path, write=true) do io
            write(io, """
            data_config_version=1

            [[datasets]]
            description="A"
            name="a_text_file"
            uuid="b498f769-a7f6-4f67-8d74-40b770398f26"

                [datasets.storage]
                driver="FileSystem"
                type="File"
                path="data/file.txt"
            """)
        end

        proj = TomlFileDataProject(data_toml_path)
        @testset "config!(proj, ...)" begin
            @test dataset(proj, "a_text_file").description == "A"
            config!(proj, "a_text_file", description="B")
            config!(proj, "a_text_file", tags=Any["xx", "yy"])
            @test dataset(proj, "a_text_file").description == "B"
            @test dataset(proj, "a_text_file").tags == ["xx", "yy"]
        end

        @testset "Persistence on disk" begin
            proj2 = TomlFileDataProject(data_toml_path)
            @test dataset(proj2, "a_text_file").description == "B"
            @test dataset(proj2, "a_text_file").tags == ["xx", "yy"]
        end

        @testset "config! via DataSet instances" begin
            ds = dataset(proj, "a_text_file")
            config!(ds, description = "C")
            @test dataset(proj, "a_text_file").description == "C"
            ds.description = "D"
            @test dataset(proj, "a_text_file").description == "D"
        end

        @testset "description and tags validation" begin
            ds = dataset(proj, "a_text_file")
            @test_throws Exception config!(ds, description = 1)
            @test_throws Exception config!(ds, tags = "hi")
        end

        @testset "global config! methods" begin
            empty!(DataSets.PROJECT)
            pushfirst!(DataSets.PROJECT, TomlFileDataProject(data_toml_path))

            config!("a_text_file", description="X")
            @test dataset("a_text_file").description == "X"

            empty!(DataSets.PROJECT)
        end
    end
end
