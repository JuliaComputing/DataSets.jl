var documenterSearchIndex = {"docs":
[{"location":"design/#What-is-a-DataSet?","page":"Design Discussion","title":"What is a DataSet?","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"In this library, a DataSet is lightweight declarative metadata describing \"any\" data in enough detail to store and load from it and to reflect it into the user's program.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Which types of metadata go in a DataSet declaration? The design principle here is to describe what and where the data is, but not how to open it. This is desirable because several modules with different design tradeoffs may open the same data.  Yet, it should be possible to share metadata between these. To sharpen the distinction of what vs how, imagine using DataSet metadata from a language other than Julia. If this makes no sense, perhaps it is how rather than what.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"There's two primary things we need to know about data:","category":"page"},{"location":"design/#Data-set-type-and-encoding","page":"Design Discussion","title":"Data set type and encoding","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Data exists outside your program in many formats. We can reflect this into the program as a particular data structure with a particular Julia type, but there's rarely a unique mapping. Therefore, we should have some concept of data set type which is independent from the types used in a Julia program. Here we'll use the made up word dtype to avoid confusion with Julia's builtin DataType.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"As an example of this non-uniqueness, consider the humble blob — a 1D array of bytes; the content of an operating system file. This data resource can be reflected into a Julia program in many ways:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"As an IO object; concretely IOStream as you'd get from a call to open().\nAs an array of bytes; concretely Vector{UInt8}, as you might get from a call to mmap().\nAs a String as you'd get from a call to read(filename, String).\nAs a path object of some kind, for example, FilePathsBase.PosixPath.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Which of these is most appropriate depends on context and must be expressed in the program code.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Conversely, the program may want to abstract over dtype, accessing many different dtypes through a common Julia type. For example, consider the problem of loading images files. There's hundreds of image formats in existence and it can be useful to map these into a single image data type for manipulation on the Julia side. So we could have dtype of JPEG, PNG and TIFF but on the Julia side load all these as Matrix{<:RGB}.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Sometimes data isn't self describing enough to decode it without extra context so we may need to include information about data encoding.  A prime example of this is the many flavous of CSV.","category":"page"},{"location":"design/#Data-storage-drivers-and-resource-locations","page":"Design Discussion","title":"Data storage drivers and resource locations","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Data may be accessed via many different mechanisms. Each DataSet needs to specify the storage driver to allow for this. Some examples:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"The local filesystem\nHTTP\nA git server\nCloud storage like Amazon S3 and Google Drive\nResearch data management servers\nRelational databases","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"To create a connection to the storage and access data we need configuration such as","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"resource location, for example the path to a file on disk\nversion information for drivers which support data versioning\ncaching strategy for remote or changing data sources","category":"page"},{"location":"design/#Other-metadata","page":"Design Discussion","title":"Other metadata","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"There's other fields which could be included, for example","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"unique identifier to manage identity in settings where the same data resides on multiple storage systems\nA default name for easy linking to the data project\nA description containing freeform text, used to document the data and as content for search systems.","category":"page"},{"location":"design/#Connecting-data-with-code","page":"Design Discussion","title":"Connecting data with code","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Having a truly useful layer for connecting data with code is surprisingly subtle. This is likely due to the many-to-many relationship between dtype vs DataType, as elaborated above.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"On the one hand, the user's code should declare which Julia types (or type traits?) it's willing to consume. On the other hand, the data should declare which dtype it consists of. Somehow we need to arrange dispatch so that two type systems can meet.","category":"page"},{"location":"design/#Data-Projects","page":"Design Discussion","title":"Data Projects","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"For using multiple datasets together — for example, in a scientific project — we'd like a DataProject type. A DataProject is a binding of convenient names to DataSets. Perhaps it also maintains the serialized DataSet information as well for those datasets which are not registered. It might be stored in a Data.toml, in analogy to Project.toml.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Maintaince of the data project should occur via a data REPL.","category":"page"},{"location":"design/#Data-Registries","page":"Design Discussion","title":"Data Registries","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"For people who'd like to share curated datasets publicly, we should have the concept of a data registry.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"The normal package system could be a reasonable way to do this to start with. We have some precedent here in packages like RDatasets, VegaDatasets, GeoDatasets, etc.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"The idea would be for the package to distribute the Data.toml metadata and any special purpose data loading code. Then hand this configuration over to DataSets.jl to provide a coherent and integrated interface to the data. Including lifecycle, downloading, caching, etc.","category":"page"},{"location":"design/#Data-REPL","page":"Design Discussion","title":"Data REPL","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"We should have a data REPL!","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"What's it for?","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Manipulating the current data project\nListing available datasets in data project\nConveniently creating DataSets, eg linking and unlinking existing local data into the data project\nCopying/caching data between storage locations\n...","category":"page"},{"location":"design/#Data-Lifecycle","page":"Design Discussion","title":"Data Lifecycle","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"For example, open and close verbs for data, caching, garbage collection, etc.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"open/close are important events which allow us to:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Create and commit versions\nCreate and record provenance information\nUpdate metadata, such as timestamps (eg for loosely coupled dataflows)","category":"page"},{"location":"design/#Versioning-and-mutability","page":"Design Discussion","title":"Versioning and mutability","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Versions are necessarily managed by the data storage backend. For example:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"A dataset which is a store of configuration files in a git repo. In this case it's git which manages versions, and the repo which stores the version history.\nFiles on a filesystem have no versioning; the data is always the latest. And this can be a fine performance tradeoff for large or temporary data.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"However the DataSet should be able to store version constraints or track the \"latest\" data in some way. In terms of git concepts, this could be","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Pinning to a particular version tag\nFollowing the master branch","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Versioning should be tied into the data lifecycle. Conceptually, the following code should","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Check that the tree is clean (if it's not, emit an error)\nCreate a tree data model and pass it to the user as git_tree\nCommit a new version using the changes made to git_tree.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"open(dataset(\"some_git_tree\"), write=true) do git_tree\n    # write or modify files in `git_tree`\n    open(joinpath(git_tree, \"some_blob.txt\"), write=true) do io\n        write(io, \"hi\")\n    end\nend","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"There's at least two quite different use patterns for versioning:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Batch update: the entire dataset is rewritten. A bit like open(filename, write=true, read=false). Your classic batch-mode application would function in this mode. You'd also want this when applying updates to the algorithm.\nIncremental update: some data is incrementally added or removed from the dataset. A bit like open(filename, read=true, write=true). You'd want to use this pattern to support differential dataflow: The upstream input dataset(s) have a diff applied; the dataflow system infers how this propagates, with the resulting patch applied to the output datasets.","category":"page"},{"location":"design/#Provenance:-What-is-this-data?-What-was-I-thinking?","page":"Design Discussion","title":"Provenance: What is this data? What was I thinking?","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Working with historical data can be confusing and error prone because the origin of that data may look like this:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"(Image: [xkcd 1838](https://xkcd.com/1838))","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"The solution is to systematically record how data came to be, including input parameters and code version. This data provenance information comes from your activity as encoded in a possibly-interactive program, but must be stored alongside the data.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"A full metadata system for data provenance is out of scope for DataSets.jl — it's a big project in its own right. But I think we should arrange the data lifecycle so that provenance can be hooked in easily by providing:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Data lifecycle events which can be used to trigger the generation and storage of provenance metadata.\nA standard entry point to user code, which makes output datasets aware of input datasets.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Some interesting links about provenance metadata:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Watch this talk: Intro to PROV by Nicholas Car: https://www.youtube.com/watch?v=elPcKqWoOPg\nThe PROV primer: https://www.w3.org/TR/2013/NOTE-prov-primer-20130430/#introduction\nhttps://www.ands.org.au/working-with-data/publishing-and-reusing-data/data-provenance","category":"page"},{"location":"design/#Data-Models","page":"Design Discussion","title":"Data Models","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"The Data Model is the abstraction which the dataset user interacts with. In general this can be provided by some arbitrary Julia code from an arbitrary module. We'll need a way to map the DataSet into the code which exposes the data model.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Examples, including some example storage formats which the data model might overlay","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Path-indexed tree-like data (Filesystem, Git, S3, Zip, HDF5)\nArrays (raw, HDF5+path, .npy, many image formats, geospatial rasters on WMTS)\nBlobs (the unstructured vector of bytes)\nTables (csv, tsv, parquet)\nJulia objects (JLD / JLD2 / serialize output)","category":"page"},{"location":"design/#Distributed-and-incremental-processing","page":"Design Discussion","title":"Distributed and incremental processing","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"For distributed or incremental processing of large data, it must be possible to load data lazily and in parallel: no single node in the computation should need the whole dataset to be locally accessible.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Not every data model can support efficient parallel processing. But for those that do it seems that the following concepts are important:","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"keys - the user works in terms of keys, eg, the indices of an array, the elements of a set, etc.\nindices - allow data to be looked up via the keys, quickly.\npartitions - large datasets must be partitioned across machines (distributed processing) or time (incremental processing with lazy loading).  The user may not want to know about this but the scheduler does.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"To be clear, DataSets largely doesn't provide these things itself — these are up to implementations of particular data models. But the data lifecycle should be designed to efficiently support distributed computation.","category":"page"},{"location":"design/#Tree-indexed-data","page":"Design Discussion","title":"Tree-indexed data","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"This is one particular data model which I've tackle this as a first use case, as a \"hieracical tree of data\" is so common. Examples are","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"The filesystem - See DataSets.FileTree\ngit - See DataSets.GitTree\nZip files - See ZipFileTree\nS3\nHDF5","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"But we don't have a well-defined path tree abstraction which already exists! So I've been prototyping some things in this package. (See also FileTrees.jl which is a new and very recent package tackling similar things.)","category":"page"},{"location":"design/#Paths-and-Roots","page":"Design Discussion","title":"Paths and Roots","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"What is a tree root object? It's a location for a data resource, including enough information to open that resource. It's the thing which handles the data lifecycle events on the whole tree.","category":"page"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"What is a relative path, in general? It's a key into a heirarchical tree-structured data store. This consists of several path components (an array of strings)","category":"page"},{"location":"design/#Iteration","page":"Design Discussion","title":"Iteration","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Fundamentally about iteration over tree nodes\nIteration over a tree yields a list of children. Children may be:\nAnother tree; isdir(child) == true\nLeaf data","category":"page"},{"location":"design/#Interesting-related-projects","page":"Design Discussion","title":"Interesting related projects","text":"","category":"section"},{"location":"design/","page":"Design Discussion","title":"Design Discussion","text":"Pkg.Artifacts solves the problem of downloading \"artifacts\": immutable containers of content-addressed tree data. Designed for the needs of distributing compiled libraries as dependencies of Julia projects, but can be used for any tree-structured data.\nDataDeps.jl solves the data downloading problem for static remote data.\nRemoteFiles.jl Downloads files from the internet and keeps them updated.\npyarrow.dataset is restricted to tabular data, but seems similar in spirit to DataSets.jl.\nFileTrees.jl provides tools for representing and processing tree-structured data lazily and in parallel.","category":"page"},{"location":"reference/#API-Reference","page":"API Reference","title":"API Reference","text":"","category":"section"},{"location":"reference/#Data-configuration","page":"API Reference","title":"Data configuration","text":"","category":"section"},{"location":"reference/","page":"API Reference","title":"API Reference","text":"dataset\nDataSet\nDataSets.DataProject\nDataSets.load_project","category":"page"},{"location":"reference/#DataSets.DataSet","page":"API Reference","title":"DataSets.DataSet","text":"A DataSet is a metadata overlay for data held locally or remotely which is unopinionated about the underlying storage mechanism.\n\nThe data in a DataSet has a type which implies an index; the index can be used to partition the data for processing.\n\n\n\n\n\n","category":"type"},{"location":"reference/#DataSets.DataProject","page":"API Reference","title":"DataSets.DataProject","text":"DataProject\n\nA data project is a collection of DataSets with associated names. Names are unique within the project.\n\n\n\n\n\n","category":"type"},{"location":"reference/#Data-function-entry-points","page":"API Reference","title":"Data function entry points","text":"","category":"section"},{"location":"reference/","page":"API Reference","title":"API Reference","text":"@datafunc\n@datarun","category":"page"},{"location":"reference/#DataSets.@datafunc","page":"API Reference","title":"DataSets.@datafunc","text":"@datafunc function f(x::DT=>T, y::DS=>S...)\n    ...\nend\n\nDefine the function f(x::T, y::S, ...) and add data dispatch rules so that f(x::DataSet, y::DataSet) will open datasets matching dataset types DT,DS as Julia types T,S.\n\n\n\n\n\n","category":"macro"},{"location":"reference/#DataSets.@datarun","page":"API Reference","title":"DataSets.@datarun","text":"@datarun [proj] func(args...)\n\nRun func with the named DataSets from the list args.\n\nExample\n\nLoad DataSets named a,b as defined in Data.toml, and pass them to f().\n\nproj = DataSets.load_project(\"Data.toml\")\n@datarun proj f(\"a\", \"b\")\n\n\n\n\n\n","category":"macro"},{"location":"reference/#Data-Models","page":"API Reference","title":"Data Models","text":"","category":"section"},{"location":"reference/","page":"API Reference","title":"API Reference","text":"File\nFileTree\nnewfile\nnewdir","category":"page"},{"location":"#DataSets.jl","page":"Introduction","title":"DataSets.jl","text":"","category":"section"},{"location":"","page":"Introduction","title":"Introduction","text":"DataSets.jl exists to help manage data and reduce the amount of data wrangling code you need to write. It's annoying to constantly rewrite","category":"page"},{"location":"","page":"Introduction","title":"Introduction","text":"Command line wrappers which deal with paths to data storage\nCode to load and save from various data storage systems (eg, local filesystem data; local git data, downloaders for remote data over various protocols, etc)\nCode to load the same data model from various serializations (eg, text: plain/compressed, property tree: toml/json/msgpack/bson/... tabular: csv/csv.gz/parquet/sqlite/...)\nCode to deal with data lifecycle; versions, provenance, etc","category":"page"},{"location":"","page":"Introduction","title":"Introduction","text":"DataSets provides scaffolding to make this kind of code more reusable. We want to make it easy to relocate an algorithm between different data environments without code changes. For example from your laptop to the cloud, to another user's machine, or to an HPC system.","category":"page"},{"location":"","page":"Introduction","title":"Introduction","text":"DataSets.jl is in early development! We're still figuring out the basic shape of the design. So things will change, but your input is important: we need your use cases so that the design serves the real needs of people dealing with data.","category":"page"},{"location":"tutorial/#Tutorial","page":"Tutorial","title":"Tutorial","text":"","category":"section"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"DocTestSetup = quote\n    using DataSets\n    project = DataSets.load_project(\"src/Data.toml\")\nend","category":"page"},{"location":"tutorial/#Declaring-DataSet-Metadata","page":"Tutorial","title":"Declaring DataSet Metadata","text":"","category":"section"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"To declare data, we create an entry in a TOML file and add some metadata.  This is fairly cumbersome right now, but in the future a data REPL will mean you don't need to do this by hand.","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"For now, we'll call our TOML file Data.toml and add the following content:","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"using Markdown\nMarkdown.parse(\"\"\"\n```toml\n$(read(\"Data.toml\",String))\n```\n\"\"\")","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"Next, we can load this declarative configuration into our Julia session as a new DataProject which is just a collection of named DataSets.","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"julia> project = DataSets.load_project(\"src/Data.toml\")\nDataProject:\n  a_text_file    => b498f769-a7f6-4f67-8d74-40b770398f26\n  a_tree_example => e7fd7080-e346-4a68-9ca9-98593a99266a","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"The DataSet metadata can be retrieved from the project using the dataset function:","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"julia> dataset(project, \"a_text_file\")\nname = \"a_text_file\"\nuuid = \"b498f769-a7f6-4f67-8d74-40b770398f26\"\ndescription = \"A text file containing the standard greeting\"\n\n[storage]\ndriver = \"FileSystem\"\ntype = \"Blob\"\npath = \"/home/chris/.julia/dev/DataSets/docs/src/data/file.txt\"","category":"page"},{"location":"tutorial/#Loading-Data","page":"Tutorial","title":"Loading Data","text":"","category":"section"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"Now that we've loaded a project, we can load the data itself. For example, to read the dataset named \"a_text_file\" as a String,","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"julia> open(String, dataset(project, \"a_text_file\"))\n\"Hello world!\\n\"","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"It's also possible to open this data as an IO stream, in which case the do block form should be used:","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"julia> open(IO, dataset(project, \"a_text_file\")) do io\n           content = read(io, String)\n           @show content\n           nothing\n       end\ncontent = \"Hello world!\\n\"","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"Let's also look at the tree example using the tree data type DataSets.FileTree:","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"julia> open(FileTree, dataset(project, \"a_tree_example\"))\n📂 Tree  @ DataSets.FileSystemRoot(\"/home/chris/.julia/dev/DataSets/docs/src/data/csvset\", true, false)\n 📄 1.csv\n 📄 2.csv","category":"page"},{"location":"tutorial/#Program-Entry-Points","page":"Tutorial","title":"Program Entry Points","text":"","category":"section"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"Rather than manually using the open() functions as shown above, the @datafunc macro lets you define entry points where DataSets will be mapped into your program.","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"For example, here we define an entry point called main which takes","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"DataSet type Blob, presenting it as a String within the program\nDataSet type Tree, presenting it as a FileTree within the program","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"The @datarun macro allows you to call such program entry points, extracting named data sets from a given project.","category":"page"},{"location":"tutorial/","page":"Tutorial","title":"Tutorial","text":"julia> @datafunc function main(x::Blob=>String, t::Tree=>FileTree)\n           @show x\n           open(String, t[\"1.csv\"]) do csv_data\n               @show csv_data\n           end\n       end\nmain (generic function with 2 methods)\n\njulia> @datarun project main(\"a_text_file\", \"a_tree_example\");\nx = \"Hello world!\\n\"\ncsv_data = \"Name,Age\\n\\\"Aaron\\\",23\\n\\\"Harry\\\",42\\n\"","category":"page"},{"location":"tutorial/#File-and-FileTree-types","page":"Tutorial","title":"File and FileTree types","text":"","category":"section"}]
}
