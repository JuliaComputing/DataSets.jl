# API Reference

## Using datasets

The primary mechanism for loading datasets is the `dataset` function, coupled
with `open()` to open the resulting `DataSet` as some Julia type. In addition,
DataSets.jl provides two macros [`@datafunc`](@ref) and [`@datarun`](@ref) to
help in creating program entry points and running them.

```@docs
dataset
@datafunc
@datarun
```

## Data environment

The global data environment for the session is defined by
[`DataSets.PROJECT`](@ref) which is initialized from the `JULIA_DATASETS_PATH`
environment variable. To load a data project from a particular TOML file, use
`DataSets.load_project`.

```@docs
DataSets.PROJECT
DataSets.load_project
DataSets.load_project!
```

## DataSet metadata model

The [`DataSet`](@ref) is a holder for dataset metadata, including the type of
the data and the method for access (the storage driver - see [Storage
Drivers](@ref)). `DataSet`s are managed in projects which may be stacked
together. The library provides several subtypes of
[`DataSets.AbstractDataProject`](@ref) for this purpose which are listed below.
(Most users will simply to configure the global data project via
[`DataSets.PROJECT`](@ref).)


```@docs
DataSet
DataSets.AbstractDataProject
DataSets.DataProject
DataSets.StackedDataProject
DataSets.ActiveDataProject
DataSets.TomlFileDataProject
```

## Data Models for files and directories

DataSets provides some builtin data models [`File`](@ref) and
[`FileTree`](@ref) for accessin file- and directory-like data respectively. For
modifying these, the functions [`newfile`](@ref) and [`newdir`](@ref) can be
used.

```@docs
File
FileTree
newfile
newdir
```

## Storage Drivers

To add a new kind of data storage backend, implement [`DataSets.add_storage_driver`](@ref)

```@docs
DataSets.add_storage_driver
```
