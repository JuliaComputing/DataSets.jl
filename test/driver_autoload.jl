@testset "Automatic code loading for drivers" begin
    empty!(DataSets.PROJECT)
    pushfirst!(LOAD_PATH, abspath("drivers"))
    ENV["JULIA_DATASETS_PATH"] = joinpath(@__DIR__, "DriverAutoloadData.toml")
    DataSets.__init__()
    @test haskey(DataSets._storage_drivers, "DummyTomlStorage")

    @test open(String, dataset("dummy_storage_blob")) == "data_from_dummy_backend"
end
