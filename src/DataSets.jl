module DataSets

using UUIDs
using TOML
using SHA
using ResourceContexts
using Base: PkgId

export DataSet, dataset, @datafunc, @datarun
export Blob, BlobTree, newfile, newdir

"""
The current DataSets version number
"""
const PACKAGE_VERSION = let
    project = TOML.parsefile(joinpath(pkgdir(DataSets), "Project.toml"))
    VersionNumber(project["version"])
end

"""
`CURRENT_DATA_CONFIG_VERSION` is the current version of the data configuration
format, as reflected in the Data.toml `data_config_version` key. This allows old
versions of DataSets.jl to detect when the Data.toml schema has changed.

New versions of DataSets.jl should always try to parse old versions of
Data.toml where possible.

### Version 0 (DataSets <= 0.2.3):

Required structure:

```toml
data_config_version = 0

[[datasets]]
name = "alphnumeric and underscore chars"
uuid = "a uuid"

[datasets.storage]
    driver = "StorageDriver"
```

### Version 1 (DataSets 0.2.4):

Same as version 0 with additions
* Allows the `/` character in dataset names to serve as a namespace separator.
* Adds a new `[[drivers]]` section with the format

```toml
[[drivers]]
type="storage"
name="<driver name>"

    [drivers.module]
    name="<module name>"
    uuid="<module uuid>"
```
"""
const CURRENT_DATA_CONFIG_VERSION = 1

include("paths.jl")
include("DataSet.jl")
include("data_project.jl")
include("file_data_projects.jl")
include("storage_drivers.jl")

#-------------------------------------------------------------------------------
# Global datasets configuration for current Julia session

function expand_project_path(path)
    if path == "@"
        return path
    elseif path == ""
        return joinpath(homedir(), ".julia", "datasets", "Data.toml")
    else
        path = abspath(expanduser(path))
        if isdir(path)
            path = joinpath(path, "Data.toml")
        end
    end
    path
end

function data_project_from_path(path)
    if path == "@"
        ActiveDataProject()
    else
        TomlFileDataProject(expand_project_path(path))
    end
end

function create_project_stack(env)
    stack = []
    env_search_path = get(env, "JULIA_DATASETS_PATH", nothing)
    if isnothing(env_search_path)
        paths = ["@", ""]
    else
        paths = isempty(env_search_path) ? String[] :
            split(env_search_path, Sys.iswindows() ? ';' : ':')
    end
    for path in paths
        project = data_project_from_path(path)
        push!(stack, project)
    end
    StackedDataProject(stack)
end

# Global stack of data projects, with the top of the stack being searched
# first.
"""
`DataSets.PROJECT` contains the default global data environment for the Julia
process. This is created from the `JULIA_DATASETS_PATH` environment variable at
initialization which is a list of paths (separated by `:` or `;` on windows).

In analogy to `Base.LOAD_PATH` and `Base.DEPOT_PATH`, the path components are
interpreted as follows:

 - `@` means the path of the current active project as returned by
   `Base.active_project(false)` This can be useful when you're "doing
   scripting" and you've got a project-specific Data.toml which resides
   next to the Project.toml. This only applies to projects which are explicitly
   set with `julia --project` or `Pkg.activate()`.
 - Explicit paths may be either directories or files in Data.toml format.
   For directories, the filename "Data.toml" is implicitly appended.
   `expanduser()` is used to expand the user's home directory.
 - As in `DEPOT_PATH`, an *empty* path component means the user's default
   Julia home directory, `joinpath(homedir(), ".julia", "datasets")`

This simplified version of the code loading rules (LOAD_PATH/DEPOT_PATH) is
used as it seems unlikely that we'll want data location to be version-
dependent in the same way that that code is.

Unlike `LOAD_PATH`, `JULIA_DATASETS_PATH` is represented inside the program as
a `StackedDataProject`, and users can add custom projects by defining their own
`AbstractDataProject` subtypes.

Additional projects may be added or removed from the stack with `pushfirst!`,
`push!` and `empty!`.
"""
PROJECT = StackedDataProject()

# deprecated. TODO: Remove dependency on this from JuliaHub
_current_project = DataProject()

_isprecompiling() = ccall(:jl_generating_output, Cint, ()) == 1

function __init__()
    # Triggering Base.require for storage drivers during precompilation should
    # be unnecessary and can cause problems if those driver modules use
    # Requires-like code loading.
    if !_isprecompiling()
        global PROJECT = create_project_stack(ENV)
        for proj in PROJECT.projects
            try
                add_storage_driver(proj)
            catch exc
                @error "Could not load storage drivers from data project" #=
                    =# project=proj exception=(exc,catch_backtrace())
            end
        end
    end
end

"""
    dataset(name)
    dataset(project, name)

Returns the [`DataSet`](@ref) with the given `name` from `project`. If omitted,
the global data environment [`DataSets.PROJECT`](@ref) will be used.

The `DataSet` is *metadata*, but to use the actual *data* in your program you
need to use the `open` function to access the `DataSet`'s content as a given
Julia type.

`name` is the name of the dataset, or more generally a "data specification": a
URI-like object of the form `namespace/name?params#fragment`.

# Example

To open a dataset named `"a_text_file"` and read the whole content as a String,

```julia
content = open(String, dataset("a_text_file"))
```

To open the same dataset as an `IO` stream and read only the first line,

```julia
open(IO, dataset("a_text_file")) do io
    line = readline(io)
    @info "The first line is" line
end
```

To open a directory as a browsable tree object,

```julia
open(BlobTree, dataset("a_tree_example"))
```
"""
dataset(name) = dataset(PROJECT, name)

"""
    load_project!(path_or_config)

Prepends to the default global dataset search stack, [`DataSets.PROJECT`](@ref).

May be renamed in a future version.
"""
function load_project!(path_or_config)
    new_project = load_project(path_or_config, auto_update=true)
    add_storage_driver(new_project)
    pushfirst!(PROJECT, new_project)
    # deprecated: _current_project reflects only the initial version of the
    # project on *top* of the stack.
    _current_project = DataProject(new_project)
end


#-------------------------------------------------------------------------------
# Application entry points
include("entrypoint.jl")

# Builtin Data models
include("BlobTree.jl")

# Builtin backends
include("filesystem.jl")
include("TomlDataStorage.jl")

# Backends
# include("ZipTree.jl")
# include("GitTree.jl")

# Application-level stuff
include("repl.jl")

end
