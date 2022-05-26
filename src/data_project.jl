# AbstractDataProject and the generic DataProject

"""
Subtypes of `AbstractDataProject` have the interface

Must implement:
  - `Base.get(project, dataset_name, default)` — search
  - `Base.keys(project)` - get dataset names

Optional:
  - `Base.iterate()`  — default implementation in terms of `keys` and `get`
  - `Base.pairs()`    — default implementation in terms of `keys` and `get`
  - `Base.haskey()`   — default implementation in terms of `get`
  - `Base.getindex()` — default implementation in terms of `get`
  - `DataSets.project_name()` — returns `nothing` by default.

Provided by AbstractDataProject (should not be overridden):
  - `DataSets.dataset()` - implemented in terms of `get`
"""
abstract type AbstractDataProject end

function Base.getindex(proj::AbstractDataProject, name::AbstractString)
    data = get(proj, name, nothing)
    data != nothing || error("DataSet $(repr(name)) not found")
    data
end

function dataset(proj::AbstractDataProject, spec::AbstractString)
    namestr, query, fragmentstr = _split_dataspec(spec)

    if isnothing(namestr)
        throw(ArgumentError("Invalid dataset specification: $spec"))
    end

    dataset = proj[namestr]

    if isnothing(query) && isnothing(fragmentstr)
        return dataset
    end

    # Enhance dataset with "dataspec" holding URL-like fragment & query
    dataspec = Dict()
    if !isnothing(query)
        dataspec["query"] = Dict{String,Any}(query)
    end
    if !isnothing(fragmentstr)
        dataspec["fragment"] = fragmentstr
    end

    # We need to take care here with copy() to avoid modifying the original
    # dataset configuration.
    conf = copy(dataset.conf)
    conf["dataspec"] = dataspec

    # FIXME: This copy is problematic now that datasets can be mutated with
    # `DataSets.config!()` as "dataspec" will infect the dataset when it's
    # saved again.
    return DataSet(data_project(dataset), conf)
end

"""
    config!(name::AbstractString; kws...)
    config!(proj::AbstractDataProject, name::AbstractString; kws...)

    config!(dataset::DataSet; kws...)

Update the configuration of `dataset` with the given keyword arguments and
persist it in the dataset's project storage. The versions which take a `name`
use that name to search within the given data project.

# Examples

Update the dataset with name "SomeData" in the global project
```
DataSets.config!("SomeData"; description="This is a description")
```

Tag the dataset "SomeData" with tags "A" and "B".
```
ds = dataset("SomeData")
DataSets.config!(ds, tags=["A", "B"])
```
"""
function config!(project::AbstractDataProject, name::AbstractString; kws...)
    config!(project[name]; kws...)
end

# Percent-decode a string according to the URI escaping rules.
# Vendored from URIs.jl for now to avoid depending on that entire package for
# this one function.
function _unescapeuri(str)
    occursin("%", str) || return str
    out = IOBuffer()
    i = 1
    io = IOBuffer(str)
    while !eof(io)
        c = read(io, Char)
        if c == '%'
            c1 = read(io, Char)
            c = read(io, Char)
            write(out, parse(UInt8, string(c1, c); base=16))
        else
            write(out, c)
        end
    end
    return String(take!(out))
end

function _split_dataspec(spec::AbstractString)
    # Parse as a suffix of URI syntax
    # name/of/dataset?param1=value1&param2=value2#fragment
    m = match(r"
        ^
        ((?:[[:alpha:]][[:alnum:]_]*/?)+)  # name     - a/b/c
        (?:\?([^#]*))?                     # query    - a=b&c=d
        (?:\#(.*))?                        # fragment - ...
        $"x,
        spec)
    if isnothing(m)
        return nothing, nothing, nothing
    end
    namestr = m[1]
    query = m[2]
    fragmentstr = m[3]

    if !isnothing(query)
        query = [_unescapeuri(x)=>_unescapeuri(y) for (x,y) in split.(split(query, '&'), '=')]
    end
    if !isnothing(fragmentstr)
        fragmentstr = _unescapeuri(fragmentstr)
    end

    namestr, query, fragmentstr
end

function Base.haskey(proj::AbstractDataProject, name::AbstractString)
    get(proj, name, nothing) !== nothing
end

function Base.iterate(project::AbstractDataProject, state=nothing)
    if isnothing(state)
        ks = keys(project)
        ks_itr = iterate(ks)
    else
        (ks, ks_state) = state
        ks_itr = iterate(ks, ks_state)
    end
    if isnothing(ks_itr)
        return nothing
    end
    (k, ks_state) = ks_itr
    val = get(project, k, nothing)
    if isnothing(val)
        # val could be `nothing` if entries in the project are updated
        # concurrently. (Eg, this might happen for data projects which are
        # backed by the filesystem.)
        return iterate(project, (ks, ks_state))
    end
    (val, (ks, ks_state))
end

# Unknown size by default, due to the above get-based implementation of
# iterate, coupled with possible concurrent modification.
Base.IteratorSize(::AbstractDataProject) = Base.SizeUnknown()

function Base.pairs(proj::AbstractDataProject)
    ks = keys(proj)
    (k=>d for (k,d) in (k=>get(proj, k, nothing) for k in ks) if !isnothing(d))
end

"""
    project_name(data_project)

Return the name of the given `data_project`. Ideally this can be used to
uniquely identify the project when modifying the project stack in
`DataSets.PROJECT`. For projects which were generated from
`JULIA_DATASETS_PATH`, this will be the expanded path component.

Other types of projects will have to return something else. For example, remote
data projects may want to return a URI. For projects which have no obvious
identifier, `nothing` is returned.
"""
project_name(data_project::AbstractDataProject) = nothing

data_drivers(proj::AbstractDataProject) = []

function Base.show(io::IO, ::MIME"text/plain", project::AbstractDataProject)
    datasets = collect(pairs(project))
    summary(io, project)
    println(io, ":")
    if isempty(datasets)
        print(io, "  (empty)")
        return
    end
    sorted = sort(datasets, by=first)
    maxwidth = maximum(textwidth.(first.(sorted)))
    for (i, (name, data)) in enumerate(sorted)
        pad = maxwidth - textwidth(name)
        storagetype = get(data.storage, "type", nothing)
        icon = storagetype in ("File", "Blob")     ? '📄' :
               storagetype in ("FileTree", "BlobTree") ? '📁' :
               '❓'
        print(io, "  ", icon, ' ', name, ' '^pad, " => ", data.uuid)
        if i < length(sorted)
            println(io)
        end
    end
end

function Base.summary(io::IO, project::AbstractDataProject)
    print(io, typeof(project))
    name = project_name(project)
    if !isnothing(name)
        print(io, " [", name, "]")
    end
end

#-------------------------------------------------------------------------------
"""
    DataProject

A in-memory collection of DataSets.
"""
struct DataProject <: AbstractDataProject
    datasets::Dict{String,DataSet}
    drivers::Vector{Dict{String,Any}}
end

DataProject() = DataProject(Dict{String,DataSet}(), Vector{Dict{String,Any}}())

DataProject(project::AbstractDataProject) = DataProject(Dict(pairs(project)),
                                                        Vector{Dict{String,Any}}())

data_drivers(project::DataProject) = project.drivers

function Base.get(proj::DataProject, name::AbstractString, default)
    get(proj.datasets, name, default)
end

Base.keys(proj::DataProject) = keys(proj.datasets)

function Base.iterate(proj::DataProject, state=nothing)
    # proj.datasets iterates key=>value; need to rejig it to iterate values.
    itr = isnothing(state) ? iterate(proj.datasets) : iterate(proj.datasets, state)
    isnothing(itr) && return nothing
    (x, state) = itr
    (x.second, state)
end

function Base.setindex!(proj::DataProject, data::DataSet, name::AbstractString)
    proj.datasets[name] = data
end

#-------------------------------------------------------------------------------
"""
    StackedDataProject()
    StackedDataProject(projects)

Search stack of AbstractDataProjects, where projects are searched from the
first to last element of `projects`.

Additional projects may be added or removed from the stack with `pushfirst!`,
`push!` and `empty!`.

See also [`DataSets.PROJECT`](@ref).
"""
struct StackedDataProject <: AbstractDataProject
    projects::Vector
end

StackedDataProject() = StackedDataProject([])

data_drivers(stack::StackedDataProject) = vcat(data_drivers.(stack.projects)...)

function Base.keys(stack::StackedDataProject)
    names = []
    for project in stack.projects
        append!(names, keys(project))
    end
    unique(names)
end

function Base.get(stack::StackedDataProject, name::AbstractString, default)
    for project in stack.projects
        d = get(project, name, nothing)
        if !isnothing(d)
            return d
        end
    end
end

# API for manipulating the stack.
Base.push!(stack::StackedDataProject, project) = push!(stack.projects, project)
Base.pushfirst!(stack::StackedDataProject, project) = pushfirst!(stack.projects, project)
Base.popfirst!(stack::StackedDataProject) = popfirst!(stack.projects)
Base.pop!(stack::StackedDataProject) = pop!(stack.projects)
Base.empty!(stack::StackedDataProject) = empty!(stack.projects)

function Base.show(io::IO, mime::MIME"text/plain", stack::StackedDataProject)
    summary(io, stack)
    println(io, ":")
    for (i,project) in enumerate(stack.projects)
        # show(io, mime, project)
        # indent each project
        str = sprint(show, mime, project)
        print(io, join("  " .* split(str, "\n"), "\n"))
        i != length(stack.projects) && println(io)
    end
end


#-------------------------------------------------------------------------------
"""
    load_project(path)

Load a data project from a system `path` referring to a TOML file.

See also [`load_project!`](@ref).
"""
function load_project(path::AbstractString; auto_update=true)
    sys_path = abspath(path)
    if !auto_update
        Base.depwarn("`auto_update` is deprecated", :load_project)
    end
    TomlFileDataProject(sys_path)
end

function load_project(config::AbstractDict; kws...)
    _check_keys(config, "Data.toml", ["data_config_version"=>Integer,
                                      "datasets"=>AbstractVector])
    format_ver = config["data_config_version"]
    if format_ver > CURRENT_DATA_CONFIG_VERSION
        error("""
              data_config_version=$format_ver is newer than supported.
              Consider upgrading to a newer version of DataSets.jl
              """)
    end
    proj = DataProject()
    for dataset_conf in config["datasets"]
        dataset = DataSet(proj, dataset_conf)
        proj[dataset.name] = dataset
    end
    if haskey(config, "drivers")
        _check_keys(config, DataProject, ["drivers"=>AbstractVector])
        for driver_conf in config["drivers"]
            _check_keys(driver_conf, DataProject, ["type"=>String, "name"=>String, "module"=>Dict])
            _check_keys(driver_conf["module"], DataProject, ["name"=>String, "uuid"=>String])
            push!(proj.drivers, driver_conf)
        end
    end
    proj
end

function save_project(path::AbstractString, proj::DataProject)
    # TODO: Put this TOML conversion in DataProject ?
    conf = Dict(
        "data_config_version"=>CURRENT_DATA_CONFIG_VERSION,
        "datasets"=>[d.conf for (n,d) in proj.datasets],
        "drivers"=>proj.drivers
    )
    mktemp(dirname(path)) do tmppath, tmpio
        TOML.print(tmpio, conf)
        close(tmpio)
        mv(tmppath, path, force=true)
    end
end

function config!(name::AbstractString; kws...)
    config!(PROJECT, name; kws...)
end
