# Tutorial

```@meta
DocTestSetup = quote
    using DataSets
    project = DataSets.load_project("src/Data.toml")
end
```

## Declaring DataSet Metadata

To declare data, we create an entry in a TOML file and add some metadata. 
This is fairly cumbersome right now, but in the future a data REPL will
mean you don't need to do this by hand.

For now, we'll call our TOML file `Data.toml` and add the following content:

````@eval
using Markdown
Markdown.parse("""
```toml
$(read("Data.toml",String))
```
""")
````


Next, we can load this declarative configuration into our Julia session as a
new `DataProject` which is just a collection of named `DataSet`s.

```jldoctest
julia> project = DataSets.load_project("src/Data.toml")
DataProject:
  a_text_file    => b498f769-a7f6-4f67-8d74-40b770398f26
  a_tree_example => e7fd7080-e346-4a68-9ca9-98593a99266a
```

The `DataSet` metadata can be retrieved from the project using the `dataset`
function:

```jldoctest
julia> dataset(project, "a_text_file")
name = "a_text_file"
uuid = "b498f769-a7f6-4f67-8d74-40b770398f26"
description = "A text file containing the standard greeting"

[storage]
driver = "FileSystem"
type = "Blob"
path = "/home/chris/.julia/dev/DataSets/docs/src/data/file.txt"
```

### Loading Data

Now that we've loaded a project, we can load the data itself. For example, to
read the dataset named `"a_text_file"` as a `String`,

```jldoctest
julia> open(String, dataset(project, "a_text_file"))
"Hello world!\n"
```

It's also possible to open this data as an `IO` stream, in which case the do
block form should be used:

```jldoctest
julia> open(IO, dataset(project, "a_text_file")) do io
           content = read(io, String)
           @show content
           nothing
       end
content = "Hello world!\n"
```

Let's also look at the tree example using the tree data type `DataSets.FileTree`:

```jldoctest
julia> open(FileTree, dataset(project, "a_tree_example"))
📂 Tree  @ DataSets.FileSystemRoot("/home/chris/.julia/dev/DataSets/docs/src/data/csvset", true, false)
 📄 1.csv
 📄 2.csv
```

## Program Entry Points

Data needs to be mapped into your program in a way that the data types declared
within Data.toml match up with some types in your program. Rather than using
the `open()` functions as shown above, the `@datafunc` macro lets you do all
this in one step. You declare

```jldoctest
julia> @datafunc function main(x::Blob=>String, t::Tree=>FileTree)
           @show x
           open(String, t["1.csv"]) do csv_data
               @show csv_data
           end
       end
main (generic function with 2 methods)

julia> @datarun project main("a_text_file", "a_tree_example");
x = "Hello world!\n"
csv_data = "Name,Age\n\"Aaron\",23\n\"Harry\",42\n"
```

## File and FileTree types
