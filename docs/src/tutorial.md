# Tutorial

```@meta
DocTestSetup = quote
    using DataSets
    # Set up data environment for docs build.
    empty!(DataSets.PROJECT)
    pushfirst!(DataSets.PROJECT, DataSets.load_project("src/Data.toml"))
end
DocTestFilters = [
    r"(?<=Project: \[).*$",
    r"path =.*",
    r"@.*"
]
```

## Making a Data.toml file

Suppose you had some data which you wanted to access located in your Julia home
directory at `~/.julia/datasets` (or `joinpath(homedir(), ".julia",
"datasets")` on windows). For this tutorial we'll use the tutorial data from
the DataSets docs directory at <https://github.com/JuliaComputing/DataSets.jl/tree/master/docs/src/data>..

To make `DataSets` aware of the data, let's create a `Data.toml` file in
`joinpath(homedir(), ".julia", "datasets", "Data.toml")` and add the following
content:

````@eval
using Markdown
Markdown.parse("""
```toml
$(read("Data.toml",String))
```
""")
````

Because we've written the `Data.toml` into a default location which is searched
for by [`DataSets.PROJECT`](@ref), it will automatically become accessible in
the default global data project:

```
julia> DataSets.PROJECT
DataSets.StackedDataProject:
  DataSets.ActiveDataProject:
    (empty)
  DataSets.TomlFileDataProject [~/.julia/datasets/Data.toml]:
    a_text_file    => b498f769-a7f6-4f67-8d74-40b770398f26
    a_tree_example => e7fd7080-e346-4a68-9ca9-98593a99266a
```

The [`dataset`](@ref) function can then be used to load metadata for a
particular dataset:

```jldoctest
julia> dataset("a_text_file")
name = "a_text_file"
uuid = "b498f769-a7f6-4f67-8d74-40b770398f26"
description = "A text file containing the standard greeting"

[storage]
driver = "FileSystem"
type = "Blob"
path = ".../DataSets/docs/src/data/file.txt"
```

If you prefer to pass around the data project explicitly rather than relying on
global configuration this is also possible:

```jldoctest
julia> project = DataSets.load_project("src/Data.toml")
DataSets.DataProject:
  a_text_file    => b498f769-a7f6-4f67-8d74-40b770398f26
  a_tree_example => e7fd7080-e346-4a68-9ca9-98593a99266a

julia> dataset(project, "a_text_file")
name = "a_text_file"
uuid = "b498f769-a7f6-4f67-8d74-40b770398f26"
description = "A text file containing the standard greeting"

[storage]
driver = "FileSystem"
type = "Blob"
path = ".../DataSets/docs/src/data/file.txt"
```

## Loading Data

To load data, call the `open()` function on the `DataSet` and pass the desired
Julia type which will be returned. For example, to read the dataset named
`"a_text_file"` as a `String`,

```jldoctest
julia> open(String, dataset("a_text_file"))
"Hello world!\n"
```

It's also possible to open this data as an `IO` stream, in which case the do
block form should be used:

```jldoctest
julia> open(IO, dataset("a_text_file")) do io
           content = read(io, String)
           @show content
           nothing
       end
content = "Hello world!\n"
```

Let's also inspect the tree example using the tree data type
[`BlobTree`](@ref). Such data trees can be indexed with path components to get
at the file [`Blob`](@ref)s inside, which in turn can be `open`ed to retrieve
the data.

```jldoctest
julia> tree = open(BlobTree, dataset("a_tree_example"))
📂 Tree  @ .../DataSets/docs/src/data/csvset
 📄 1.csv
 📄 2.csv

julia> tree["1.csv"]
📄 1.csv @ .../DataSets/test/data/csvset

julia> Text(open(String, tree["1.csv"]))
Name,Age
"Aaron",23
"Harry",42
```

## Program Entry Points

Rather than manually using the `open()` functions as shown above, the
`@datafunc` macro lets you define entry points where `DataSet`s will be mapped
into your program.

For example, here we define an entry point called `main` which takes
* DataSet type `Blob`, presenting it as a `String` within the program
* DataSet type `BlobTree`, presenting it as a `BlobTree` within the program

The `@datarun` macro allows you to call such program entry points, extracting
named data sets from a given project.

```jldoctest
julia> @datafunc function main(x::Blob=>String, t::BlobTree=>BlobTree)
           @show x
           open(String, t["1.csv"]) do csv_data
               @show csv_data
           end
       end
main (generic function with 2 methods)

julia> @datarun main("a_text_file", "a_tree_example");
x = "Hello world!\n"
csv_data = "Name,Age\n\"Aaron\",23\n\"Harry\",42\n"
```

In a given program it's possible to have multiple entry points by simply
defining multiple `@datafunc` implementations. In this case `@datarun` will
dispatch to the entry point with the matching `DataSet` type.

