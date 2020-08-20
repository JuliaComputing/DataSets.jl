# DataSets

[![Build Status](https://github.com/c42f/DataSets.jl/workflows/CI/badge.svg)](https://github.com/c42f/DataSets.jl/actions)

DataSets.jl **decouples data and code** so that algorithms can work in
different data environments without changes to the code. This makes it
easy to *relocate* a computation — for example from your laptop to the
cloud, to another user's machine, or to an HPC system.

with a lightweight layer of metadata
called a `DataSet`. The `DataSet` bridges the gap between data outside stored
outside your program and the data abstractions within.



and **records how
data came to exist**. We aim to make data formats and storage pluggable so you
can connect your algorithms to new data sources without code changes. We aim to
record answers the perennial questions "What is this old data? How was it
generated!?"

## How-to

### Declare datasets

```
data> link airports https://github.com/queryverse/VegaDatasets.jl/raw/master/data/data/airports.csv

data> link mytable file:///foo/bar.csv

data> link table2 file:///foo/bar.csv.gz
```


### File and directory trees: local and remote, zipped and not

Here's an example of how to work with "files and directories" abstractly,
without needing for them to reside on the local disk.


```julia

open()
```



## Design

### Connecting data with algorithms

Every Julia program needs to handle data input and output in some form. To load
and save data, you'd typically write some special-purpose data handling code
which wraps around your algorithm and provides IO. For example, reading and
writing files to input and output directories.

However this can be inconvenient when you need to support additional data
sources and sinks. For example, decompressing from a zip file and writing the
results over the network to a blob store like Amazon S3. This makes the code
*less relocatable* between computation environments with varying data storage
mechanisms.

DataSets.jl provides abstractions for data so **the community can collaborate on
shared tools for data IO** in a way which is decoupled from user algorithms.
We want user code to function seamlessly in different data environments by
adding a little configuration.

DataSets.jl decouples data processing from the detail of IO and data storage.
This lets you write data processing code with high level data abstractions,
without worrying about the detail of file formats.

### What is this data? What was I thinking?

Working with historical data can be confusing and error prone because the
origin of that data may look like this:

![[xkcd 1838](https://xkcd.com/1838)](https://imgs.xkcd.com/comics/machine_learning.pnghttps://xkcd.com/1838)

Of course, the solution to this is to systematically record how data came to
be, including input parameters and code version. This **data provenance**
information comes from your activity as encoded in a possibly-interactive
program, but must be stored alongside the data.

DataSets provides (TODO!!) a systematic way to connect provenance metadata to
your output data, and tools to make this automatic in practical cases.

Some philosophy: I feel that keeping track of data provenance is extra
difficult in exploratory data analysis (for example, in scientific work)
because
* It requires a high level of attention to detail and this state of mind is
  intrusive on the creative mindset needed in exploratory work.
* Most output data will typically be discarded, which makes it hard to find
  immediate motivation to preserve this information.

Interesting links:
* Watch this talk: *Intro to PROV* by Nicholas Car: https://www.youtube.com/watch?v=elPcKqWoOPg
* The PROV primer: https://www.w3.org/TR/2013/NOTE-prov-primer-20130430/#introduction
* https://www.ands.org.au/working-with-data/publishing-and-reusing-data/data-provenance


## DataSets Design 

The driving force in the design of DataSets.jl is to make data and code
*relocatable* — to allow an algorithm to run across diverse data storage
and compute environments without code changes.

With the declarative data description which arises from this we're also able to
tackle data provenance and versioning.

## Design problems

### Git Trees

* How do we store paths? May store them as a string which is independent from
  the internal representation...

