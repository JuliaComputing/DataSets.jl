## What is a DataSet?

In this library, a `DataSet` is lightweight declarative metadata describing
"any" data in enough detail to store and load from it and to reflect it into
the user's program.

Which types of metadata go in a DataSet declaration? The design principle here
is to describe *what* and *where* the data is, but not *how* to open it. This
is desirable because several modules with different design tradeoffs may open
the same data.  Yet, it should be possible to share metadata between these. To
sharpen the distinction of what vs how, imagine using `DataSet` metadata from a
language other than Julia. If this makes no sense, perhaps it is *how* rather
than *what*.

There's two primary things we need to know about data:

### Data set type and encoding

Data exists outside your program in many formats. We can reflect this into the
program as a particular data structure with a particular Julia type, but
there's rarely a unique mapping. Therefore, we should have some concept of
*data set type* which is independent from the types used in a Julia program.
Here we'll use the made up word **dtype** to avoid confusion with Julia's
builtin `DataType`.

As an example of this non-uniqueness, consider the humble blob — a 1D array of
bytes; the *content* of an operating system file. This data resource can be
reflected into a Julia program in many ways:

* As an `IO` object; concretely `IOStream` as you'd get from a call to `open()`.
* As an array of bytes; concretely `Vector{UInt8}`, as you might get from a
  call to `mmap()`.
* As a `String` as you'd get from a call to `read(filename, String)`.
* As a path object of some kind, for example, `FilePathsBase.PosixPath`.

Which of these is most appropriate depends on context and must be expressed in
the program code.

Conversely, the program may want to abstract over dtype, accessing many
different dtypes through a common Julia type. For example, consider the problem
of loading images files. There's hundreds of image formats in existence and it
can be useful to map these into a single image data type for manipulation on
the Julia side. So we could have dtype of JPEG, PNG and TIFF but on the Julia
side load all these as `Matrix{<:RGB}`.

Sometimes data isn't self describing enough to decode it without extra context
so we may need to include information about **data encoding**.  A prime
example of this is the [many flavous of
CSV](https://juliadata.github.io/CSV.jl/stable/#CSV.File).

### Data storage drivers and resource locations

Data may be accessed via many different mechanisms. Each DataSet needs to
specify the *storage driver* to allow for this. Some examples:
* The local filesystem
* HTTP
* A git server
* Cloud storage like Amazon S3 and Google Drive
* Research data management servers
* Relational databases

To create a connection to the storage and access data we need configuration
such as
* *resource location*, for example the path to a file on disk
* *version information* for drivers which support data versioning
* *caching strategy* for remote or changing data sources

### Other metadata

There's other fields which could be included, for example
* *unique identifier* to manage identity in settings where the same data
  resides on multiple storage systems
* A *default name* for easy linking to the data project
* A *description* containing freeform text, used to document the
  data and as content for search systems.

## Connecting data with code

Having a truly useful layer for connecting data with code is surprisingly
subtle. This is likely due to the many-to-many relationship between dtype vs
DataType, as elaborated above.

On the one hand, the user's code should declare which Julia types (or type
traits?) it's willing to consume. On the other hand, the data should declare
which dtype it consists of. Somehow we need to arrange dispatch so that two
type systems can meet.

## Data Projects

For using multiple datasets together — for example, in a scientific project —
we'd like a `DataProject` type. A `DataProject` is a binding of convenient
names to `DataSet`s. Perhaps it also maintains the serialized `DataSet`
information as well for those datasets which are not registered. It might be
stored in a Data.toml, in analogy to Project.toml.

Maintenance of the data project should occur via a data REPL.

## Data Registries

For people who'd like to share curated datasets publicly, we should have the
concept of a data registry.

The normal package system could be a reasonable way to do this to start with.
We have some precedent here in packages like RDatasets, VegaDatasets,
GeoDatasets, etc.

The idea would be for the package to distribute the Data.toml metadata and any
special purpose data loading code. Then hand this configuration over to
DataSets.jl to provide a coherent and integrated interface to the data.
Including lifecycle, downloading, caching, etc.

## Data REPL

We should have a data REPL!

What's it for?
* Manipulating the current data project
* Listing available datasets in data project
* Conveniently creating `DataSet`s, eg linking and unlinking existing local
  data into the data project
* Copying/caching data between storage locations
* ...

## Data Lifecycle

For example, `open` and `close` verbs for data, caching, garbage collection,
etc.

`open/close` are important events which allow us to:
* Create and commit versions
* Create and record provenance information
* Update metadata, such as timestamps (eg for loosely coupled dataflows)

### Versioning and mutability

Versions are necessarily managed by the data storage backend. For example:
* A dataset which is a store of configuration files in a git repo. In this case
  it's git which manages versions, and the repo which stores the version history.
* Files on a filesystem have no versioning; the data is always the latest. And
  this can be a fine performance tradeoff for large or temporary data.

However the `DataSet` should be able to store version constraints or track the
"latest" data in some way. In terms of git concepts, this could be
* Pinning to a particular version tag
* Following the master branch

Versioning should be tied into the data lifecycle. Conceptually, the following
code should
1. Check that the tree is clean (if it's not, emit an error)
2. Create a tree data model and pass it to the user as `git_tree`
3. Commit a new version using the changes made to `git_tree`.

```julia
open(dataset("some_git_tree"), write=true) do git_tree
    # write or modify files in `git_tree`
    open(joinpath(git_tree, "some_blob.txt"), write=true) do io
        write(io, "hi")
    end
end
```

There's at least two quite different use patterns for versioning:
* Batch update: the entire dataset is rewritten. A bit like
  `open(filename, write=true, read=false)`. Your classic batch-mode application
  would function in this mode. You'd also want this when applying updates to
  the algorithm.
* Incremental update: some data is incrementally added or removed
  from the dataset. A bit like `open(filename, read=true, write=true)`. You'd
  want to use this pattern to support differential dataflow: The upstream input
  dataset(s) have a diff applied; the dataflow system infers how this
  propagates, with the resulting patch applied to the output datasets.

### Provenance: What is this data? What was I thinking?

Working with historical data can be confusing and error prone because the
origin of that data may look like this:

![[xkcd 1838](https://xkcd.com/1838)](https://imgs.xkcd.com/comics/machine_learning.png)

The solution is to systematically record how data came to be, including input
parameters and code version. This *data provenance* information comes from
your activity as encoded in a possibly-interactive program, but must be stored
alongside the data.

A full metadata system for data provenance is out of scope for DataSets.jl —
it's a big project in its own right. But I think we should arrange the data
lifecycle so that provenance can be hooked in easily by providing:

* *Data lifecycle events* which can be used to trigger the generation and
  storage of provenance metadata.
* A standard entry point to user code, which makes output datasets aware of
  input datasets.

Some interesting links about provenance metadata:
* Watch this talk: *Intro to PROV* by Nicholas Car: https://www.youtube.com/watch?v=elPcKqWoOPg
* The PROV primer: https://www.w3.org/TR/2013/NOTE-prov-primer-20130430/#introduction
* https://www.ands.org.au/working-with-data/publishing-and-reusing-data/data-provenance


## Data Models

The Data Model is the abstraction which the dataset user interacts with. In
general this can be provided by some arbitrary Julia code from an arbitrary
module. We'll need a way to map the `DataSet` into the code which exposes the
data model.

Examples, including some example storage formats which the data model might
overlay
* Path-indexed tree-like data (Filesystem, Git, S3, Zip, HDF5)
* Arrays (raw, HDF5+path, .npy, many image formats, geospatial rasters on WMTS)
* Blobs (the unstructured vector of bytes)
* Tables (csv, tsv, parquet)
* Julia objects (JLD / JLD2 / `serialize` output)

### Distributed and incremental processing

For distributed or incremental processing of large data, it **must be possible
to load data lazily and in parallel**: no single node in the computation should
need the whole dataset to be locally accessible.

Not every data model can support efficient parallel processing. But for those
that do it seems that the following concepts are important:

* *keys* - the user works in terms of keys, eg, the indices of an array, the
  elements of a set, etc.
* *indices* - allow data to be looked up via the keys, quickly.
* *partitions* - large datasets must be partitioned across
  machines (distributed processing) or time (incremental processing with lazy
  loading).  The user may not want to know about this but the scheduler does.

To be clear, DataSets largely doesn't provide these things itself — these are
up to implementations of particular data models. But the data lifecycle should
be designed to efficiently support distributed computation.

### Tree-indexed data

This is one particular data model which I've tackle this as a first use case,
as a "hieracical tree of data" is so common. Examples are

* The filesystem - See `DataSets.FileTree`
* git - See `DataSets.GitTree`
* Zip files - See `ZipFileTree`
* S3
* HDF5

But we don't have a well-defined path tree abstraction which already exists! So
I've been prototyping some things in this package. (See also FileTrees.jl which
is a new and very recent package tackling similar things.)

#### Paths and Roots

What is a **tree root** object? It's a location for a data resource, including
enough information to open that resource. It's the thing which handles the data
lifecycle events on the whole tree.

What is a **relative path**, in general? It's a *key* into a heirarchical
tree-structured data store. This consists of several path *components* (an
array of strings)

#### Iteration

* Fundamentally about iteration over tree nodes
* Iteration over a tree yields a list of children. Children may be:
    * Another tree; `isdir(child) == true`
    * Leaf data

# Interesting related projects

* [Pkg.Artifacts](https://julialang.github.io/Pkg.jl/v1/artifacts/) solves the
  problem of downloading "artifacts": immutable containers of content-addressed
  tree data. Designed for the needs of distributing compiled libraries as
  dependencies of Julia projects, but can be used for any tree-structured data.
* [DataDeps.jl](https://github.com/oxinabox/DataDeps.jl) solves the data
  downloading problem for static remote data.
* [RemoteFiles.jl](https://github.com/helgee/RemoteFiles.jl) Downloads files from the internet and keeps them updated.
* [pyarrow.dataset](https://arrow.apache.org/docs/python/dataset.html)
  is restricted to tabular data, but seems similar in spirit to DataSets.jl.
* [FileTrees.jl](http://shashi.biz/FileTrees.jl) provides tools for
  representing and processing tree-structured data lazily and in parallel.
