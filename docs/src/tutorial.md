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
    r"@.*",
    r"(?<=IOStream\().*",
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
type = "File"
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
type = "File"
path = ".../DataSets/docs/src/data/file.txt"
```

## Loading Data

You can call `open()` on a DataSet to inspect the data inside.  `open()` will
return the [`File`](@ref) and [`FileTree`](@ref) types for local files and
directories on disk. For example,

```jldoctest
julia> open(dataset("a_text_file"))
📄  @ .../DataSets/docs/src/data/file.txt

julia> open(dataset("a_tree_example"))
📂 Tree  @ .../DataSets/docs/src/data/csvset
 📄 1.csv
 📄 2.csv
```

Use the form `open(T, dataset)` to read the data as a specific type. `File`
data can be opened as `String`, `IO`, or `Vector{UInt8}`, depending on your
needs:

```jldoctest
julia> io = open(IO, dataset("a_text_file"))
IOStream(<file .../DataSets/docs/src/data/file.txt>)

julia> read(io, String)
"Hello world!\n"

julia> buf = open(Vector{UInt8}, dataset("a_text_file"));

julia> String(buf)
"Hello world!\n"

julia> open(String, dataset("a_text_file"))
"Hello world!\n"
```

To ensure the dataset is closed again in a timely way (freeing any resources
such as file handles), you should use the scoped form, for example:

```jldoctest
julia> open(IO, dataset("a_text_file")) do io
           content = read(io, String)
           @show content
           nothing
       end
content = "Hello world!\n"
```

Let's look at some tree-like data which is represented on local disk as a
folder or directory. Tree data is opened in Julia as the [`FileTree`](@ref)
type and can be indexed with path components to get at the file [`File`](@ref)s
inside. In turn, we can `open()` one of the file blobs and look at the data
contained within.

```jldoctest
julia> tree = open(FileTree, dataset("a_tree_example"))
📂 Tree  @ .../DataSets/docs/src/data/csvset
 📄 1.csv
 📄 2.csv

julia> tree["1.csv"]
📄 1.csv @ .../DataSets/docs/src/data/csvset

julia> open(String, tree["1.csv"]) |> Text
Name,Age
"Aaron",23
"Harry",42
```

## Program Entry Points

Rather than manually using the `open()` functions as shown above, the
`@datafunc` macro lets you define entry points where `DataSet`s will be mapped
into your program.

For example, here we define an entry point called `main` which takes
* DataSet type `File`, presenting it as a `String` within the program
* DataSet type `FileTree`, presenting it as a `FileTree` within the program

The `@datarun` macro allows you to call such program entry points, extracting
named data sets from a given project.

```jldoctest
julia> @datafunc function main(x::File=>String, t::FileTree=>FileTree)
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

