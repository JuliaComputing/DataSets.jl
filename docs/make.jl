using Documenter, DataSets

makedocs(;
    modules=[DataSets],
    format=Documenter.HTML(),
    pages=[
        "Introduction" => "index.md",
        "Tutorial" => "tutorial.md",
        "API Reference" => "reference.md",
        "Design Discussion" => "design.md",
    ],
    repo="https://github.com/JuliaComputing/DataSets.jl/blob/{commit}{path}#L{line}",
    sitename="DataSets.jl",
    authors = "Chris Foster and contributors: https://github.com/JuliaComputing/DataSets.jl/graphs/contributors"
)

deploydocs(;
    repo="github.com/JuliaComputing/DataSets.jl",
    push_preview=true
)
