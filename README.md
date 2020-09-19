# DataSets

[![Build Status](https://github.com/c42f/DataSets.jl/workflows/CI/badge.svg)](https://github.com/c42f/DataSets.jl/actions)

DataSets.jl exists to help manage data and reduce the amount of data wrangling
code you need to write. It's annoying to write
* Command line wrappers which deal with paths to data storage
* Code to load and save from various *data storage systems* (eg, local
  filesystem data; local git data, downloaders for remote data over various
  protocols, etc)
* Code to load the same data model from various serializations (eg, text:
  plain/compressed, property tree: toml/json/msgpack/bson/...
  tabular: csv/csv.gz/parquet/sqlite/...)
* Code to deal with data lifecycle; versions, provenance, etc

DataSets provides scaffolding to make this kind of code more reusable.  We want
to make it easy to *relocate* an algorithm between different data environments
without code changes. For example from your laptop to the cloud, to another
user's machine, or to an HPC system.

**DataSets.jl is an early prototype!** We're still figuring out the basic shape of the
design. So things will change, but *your input is important*: we need your use
cases so that the design serves the real needs of people dealing with data.

# Design

Here's the rough shape of the design which is being considered.

## What is a DataSet?

A `DataSet` is lightweight metadata describing "any" data store and the code to
load and store to it. We want it to be declarative so you can refer to data
without opening it or loading the modules required to do so.

We need to describe

*Storage backend* and location. examples:
* Filesystem, S3, Git server
* Research data management servers
* Relational databases

*Data model*. Examples:
* Path-indexed tree-like data (Filesystem, Git, S3, Zip, HDF5, ...)
* Blobs (1D arrays of bytes)
* Arrays in general (eg, large geospatial rasters)
* Tables

Also many other things should be included, for example
* default name
* description
* version constraints
* cachability
* unique identifier
* ...

## Data Projects

For using multiple datasets together — for example, in a scientific project —
we'd like a `DataProject` type. A `DataProject` is a binding of convenient
names to `DataSet`s. Perhaps it also maintains the serialized `DataSet`
information as well for those datasets which are not registered. It might be
stored in a Data.toml, in analogy to Project.toml.

Maintaince of the data project should occur via a data REPL.

## Data Registries

For people who'd like to share curated datasets publically, we should have the
concept of a data registry.

We propose that the normal package system would be a reasonable way to do this;
we have some precedent here in packages like RDatasets, VegaDatasets,
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

This kind of thing already works in the prototype code - look at
`DataSets.GitTreeRoot`.

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

