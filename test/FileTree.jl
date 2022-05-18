
@testset "File API" begin
    file = File(FileSystemRoot("data/file.txt"))

    @testset "metadata" begin
        @test filesize(file) == 13
        @test isfile(file)
        @test !isdir(file)
        @test ispath(file)
    end

    @testset "open()" begin
        # Do-block based forms
        @test        open(identity, String, file)         == "Hello world!\n"
        @test String(open(identity, Vector{UInt8}, file)) == "Hello world!\n"
        @test open(io->read(io,String), IO, file)         == "Hello world!\n"
        @test open(identity, File, file) === file

        # Unscoped forms
        @test open(String, file)                == "Hello world!\n"
        @test String(open(Vector{UInt8}, file)) == "Hello world!\n"
        @test read(open(IO, file), String)      == "Hello world!\n"

        # Context-based forms
        @context begin
            @test @!(open(String, file))               == "Hello world!\n"
            @test String(@! open(Vector{UInt8}, file)) == "Hello world!\n"
            @test read(@!(open(IO, file)), String)     == "Hello world!\n"
            @test @!(open(File, file))                 === file
        end
    end
end

@testset "FileTree API" begin
    tree = FileTree(FileSystemRoot("data"))

    @testset "metadata" begin
        @test !isfile(tree)
        @test isdir(tree)
        @test ispath(tree)
    end

    @test tree["csvset"] isa FileTree
    @test tree["csvset/1.csv"] isa File
    @test tree["csvset"]["2.csv"] isa File

    @testset "open()" begin
        @test open(identity, FileTree, tree) === tree

        # Context-based forms
        @context begin
            @test @!(open(FileTree, tree)) === tree
        end
    end

    # TODO: delete!
end

@testset "newfile() and newdir()" begin
    @testset "isolated newfile" begin
        @test newfile() isa File
        @test read(newfile()) == []
        @test begin
            f = newfile() do io
                print(io, "content")
            end
            read(f, String)
        end == "content"
    end

    tree = newdir()
    for j=1:2
        d = newdir(tree, "d$j")
        for i=1:2
            newfile(d, "hi_$(j)_$(i).txt") do io
                println(io, "hi $j/$i")
            end
        end
    end
    @test read(tree["d1/hi_1_1.txt"], String) == "hi 1/1\n"
    @test read(tree["d1/hi_1_2.txt"], String) == "hi 1/2\n"
    @test read(tree["d2/hi_2_1.txt"], String) == "hi 2/1\n"
    @test read(tree["d2/hi_2_2.txt"], String) == "hi 2/2\n"

    @testset "Iteration" begin
        # keys
        @test keys(tree) == ["d1", "d2"]
        @test keys(tree["d1"]) == ["hi_1_1.txt", "hi_1_2.txt"]
        @test keys(tree["d2"]) == ["hi_2_1.txt", "hi_2_2.txt"]
        # values
        for v in tree
            @test v isa FileTree
        end
        for v in values(tree)
            @test v isa FileTree
        end
        for v in tree["d1"]
            @test v isa File
        end
        # pairs
        @test first.(pairs(tree["d1"])) == ["hi_1_1.txt", "hi_1_2.txt"]
        #@test typeof.(last.(pairs(tree["d1"]))) == [File, File]
    end

    @testset "copy / copy! for FileTree" begin
        tree2 = copy!(newdir(), tree)
        @test keys(tree2) == ["d1", "d2"]
        @test keys(tree2["d1"]) == ["hi_1_1.txt", "hi_1_2.txt"]
        @test keys(tree2["d2"]) == ["hi_2_1.txt", "hi_2_2.txt"]
        @test read(tree2["d1/hi_1_1.txt"], String) == "hi 1/1\n"

        @testset "copy! into a subtree" begin
            copy!(newdir(tree2, "dst"), tree)
            @test keys(tree2["dst"]) == ["d1", "d2"]
            @test keys(tree2["dst/d1"]) == ["hi_1_1.txt", "hi_1_2.txt"]
        end

        @testset "copy" begin
            @test keys(copy(tree)) == ["d1", "d2"]
        end
    end

    @testset "newdir/newfile with overwrite=true" begin
        tree3 = copy!(newdir(), tree)

        @test_throws ErrorException newdir(tree3, "d1")
        @test keys(tree3["d1"]) == ["hi_1_1.txt", "hi_1_2.txt"]
        newdir(tree3, "d1", overwrite=true)
        @test keys(tree3["d1"]) == []

        # Various forms of newfile
        @test newfile(tree3, "empty") isa File
        @test open(String, tree3["empty"]) == ""
        @test_throws ErrorException newfile(tree3, "empty")
        newfile(tree3, "empty", overwrite=true) do io
            print(io, "xxx")
        end
        @test open(String, tree3["empty"]) == "xxx"
        # newfile creates directories implicitly
        @test newfile(tree3, "a/b/c") isa File
        @test tree3["a"]["b"]["c"] isa File
    end
end

#=
#TODO
@testset "FileSystemRoot" begin
    # Test that the file is persisted on disk
    @test isfile(DataSets.sys_abspath(tree["d1/hi_2.txt"]))
end
=#
