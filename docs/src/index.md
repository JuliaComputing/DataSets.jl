# DataSets.jl

DataSets.jl exists to help manage data and reduce the amount of data wrangling
code you need to write. It's annoying to constantly rewrite
* Command line wrappers which deal with paths to data storage
* Code to load and save from various *data storage systems* (eg, local
  filesystem data; local git data, downloaders for remote data over various
  protocols, etc)
* Code to load the same data model from various serializations (eg, text:
  plain/compressed, property tree: toml/json/msgpack/bson/...
  tabular: csv/csv.gz/parquet/sqlite/...)
* Code to deal with data lifecycle; versions, provenance, etc

DataSets provides scaffolding to make this kind of code more reusable. We want
to make it easy to *relocate* an algorithm between different data environments
without code changes. For example from your laptop to the cloud, to another
user's machine, or to an HPC system.

**DataSets.jl is in early development!** We're still figuring out the basic
shape of the design. So things will change, but *your input is important*: we
need your use cases so that the design serves the real needs of people dealing
with data.


