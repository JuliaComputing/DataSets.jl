@testset "FileTree API" begin
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

    @testset "metadata" begin
        f = tree["d1/hi_1_1.txt"]
        @test filesize(f) == 7
        @test isfile(f)
        @test !isdir(f)
        @test ispath(f)

        d = tree["d1"]
        @test !isfile(d)
        @test isdir(d)
        @test ispath(d)

        @test haskey(tree, "d1")
        @test !haskey(tree, "x")
    end

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

    @testset "setindex!" begin
        tree = newdir()
        @test keys(tree) == []
        tree["a"] = newfile()
        @test tree["a"] isa File
        tree["b"] = newdir()
        @test tree["b"] isa FileTree
        tree["c/d"] = newfile()
        @test tree["c"] isa FileTree
        @test tree["c/d"] isa File
        @test keys(tree) == ["a","b","c"]
        d = newdir()
        newfile(io->print(io, "E"), d, "e")
        newfile(io->print(io, "F"), d, "f")
        tree["x"] = d
        @test read(tree["x/e"], String) == "E"
        @test read(tree["x/f"], String) == "F"
    end

    @testset "delete!" begin
        tree = newdir()
        newfile(tree, "a/b/c")
        newfile(tree, "a/b/d")
        @test keys(tree) == ["a"]
        delete!(tree, "a")
        @test keys(tree) == []
        newfile(tree, "x")
        @test keys(tree) == ["x"]
        delete!(tree, "x")
        @test keys(tree) == []
    end

    @testset "open(::File)" begin
        file = newfile(io->print(io, "xx"))

        # Do-block based forms
        @test        open(identity, String, file)         == "xx"
        @test String(open(identity, Vector{UInt8}, file)) == "xx"
        @test open(io->read(io,String), IO, file)         == "xx"
        @test open(identity, File, file) === file

        # Unscoped forms
        @test open(String, file)                == "xx"
        @test String(open(Vector{UInt8}, file)) == "xx"
        @test read(open(IO, file), String)      == "xx"

        # Context-based forms
        @context begin
            @test @!(open(String, file))               == "xx"
            @test String(@! open(Vector{UInt8}, file)) == "xx"
            @test read(@!(open(IO, file)), String)     == "xx"
            @test @!(open(File, file))                 === file
        end
    end

    @testset "open(::FileTree)" begin
        tree = FileTree(FileSystemRoot("data"))

        @test open(identity, FileTree, tree) === tree

        # Context-based forms
        @context begin
            @test @!(open(FileTree, tree)) === tree
        end
    end
end

@testset "newfile / newdir cleanup" begin
    f = newfile()
    global sys_file_path = f.root.path
    GC.@preserve f  @test isfile(sys_file_path)
    d = newdir()
    global sys_dir_path = d.root.path
    GC.@preserve d @test isdir(sys_dir_path)
end
# Having the following as a separate top level statement ensures that `f` and
# `d` aren't accidentally still rooted so the the GC can clean them up.
@testset "newfile / newdir cleanup step 2" begin
    GC.gc()
    @test !ispath(sys_file_path)
    @test !ispath(sys_dir_path)
end

#=
#TODO
@testset "FileSystemRoot" begin
    # Test that the file is persisted on disk
    @test isfile(DataSets.sys_abspath(tree["d1/hi_2.txt"]))
end
=#
