
@testset "open() for DataSet" begin
    proj = DataSets.load_project("Data.toml")

    blob_ds = dataset(proj, "embedded_blob")
    @test open(blob_ds) isa File
    @test open(String, blob_ds) == "\0\0\0\0\0\0E@"
    @test read(open(blob_ds), Float64) === 42.0

    @test open(IO, blob_ds) do io
        read(io, String)
    end == "\0\0\0\0\0\0E@"

    @context begin
        @test @!(open(String, blob_ds)) == "\0\0\0\0\0\0E@"

        blob = @! open(blob_ds)
        @test blob isa File
        @test @!(open(String, blob)) == "\0\0\0\0\0\0E@"

        @test read(blob, Float64) === 42.0
        @test read(blob) == UInt8[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x45, 0x40]
    end

    tree_ds = dataset(proj, "embedded_tree")
    @test open(tree_ds) isa FileTree
    @test open(String, open(tree_ds)[path"d01/a.txt"]) == "1 a content"
    @test open(String, open(tree_ds)[path"d02/b.txt"]) == "2 b content"
    @context begin
        tree = @! open(tree_ds)
        @test tree isa FileTree

        @test isdir(tree)
        @test !isfile(tree)

        @test readdir(tree) == ["d01", "d02", "d03", "d04"]
        @test readdir(tree["d01"]) == ["a.txt", "b.txt"]

        @test !isdir(tree[path"d01/a.txt"])
        @test isfile(tree[path"d01/a.txt"])

        @test_throws ErrorException tree[path"nonexistent/a/b"]
        @test_throws ErrorException tree["nonexistent"]

        @test @!(open(String, tree[path"d01/a.txt"])) == "1 a content"
        @test @!(open(String, tree[path"d02/b.txt"])) == "2 b content"
    end
end

