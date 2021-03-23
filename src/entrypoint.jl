#-------------------------------------------------------------------------------
# Entry point utilities
#
# These make it easy for users to open `DataSet`s and map them into types
# understood by their program.

function extract_dtypes(call)
    dtypes = []
    jtypes = []
    argnames = []
    for ex in call.args[2:end]
        @assert ex.head == :call && ex.args[1] == :(=>)
        @assert ex.args[2].head == :(::) && length(ex.args[2].args) == 2
        push!(argnames, ex.args[2].args[1])
        push!(dtypes, ex.args[2].args[2])
        push!(jtypes, ex.args[3])
    end
    argnames, dtypes, jtypes
end

"""
    @datafunc function f(x::DT=>T, y::DS=>S...)
        ...
    end

Define the function `f(x::T, y::S, ...)` and add data dispatch rules so that
`f(x::DataSet, y::DataSet)` will open datasets matching dataset types `DT,DS`
as Julia types `T,S`.
"""
macro datafunc(func_expr)
    @assert func_expr.head == :function
    call = func_expr.args[1]
    body = func_expr.args[2]
    funcname = call.args[1]
    argnames, dtypes, jtypes = extract_dtypes(call)
    real_args = [:($n::$t) for (n,t) in zip(argnames, jtypes)]
    table_name = Symbol("#_$(funcname)_datasets_dispatch")
    esc_funcname = esc(funcname)
    esc_table_name = esc(table_name)
    func_expr.args[1].args[2:end] = real_args
    quote
        if !$(esc(:(@isdefined($table_name))))
            function $esc_funcname(ds::DataSet...)
                _run($esc_funcname, $esc_table_name, ds...)
            end
            const $esc_table_name = Dict()
        end
        push!($esc_table_name, tuple($(map(string, dtypes)...)) =>
                               tuple($(map(esc, jtypes)...)))
        $(esc(func_expr))
    end
end

function datarun(proj::DataProject, func::Function, data_names::AbstractString...)
    ds = map(n->dataset(proj, n), data_names)
    func(ds...)
end

"""
    @datarun [proj] func(args...)

Run `func` with the named `DataSet`s from the list `args`.

# Example

Load `DataSet`s named a,b as defined in Data.toml, and pass them to `f()`.
```
proj = DataSets.load_project("Data.toml")
@datarun proj f("a", "b")
```
"""
macro datarun(args...)
    if length(args) == 2
        proj, call = args
        esc_proj = esc(proj)
    elseif length(args) == 1
        esc_proj = :_current_project
        call = args[1]
    else
        throw(ArgumentError("@datarun macro expects one or two arguments"))
    end
    esc_funcname = esc(call.args[1])
    esc_funcargs = esc.(call.args[2:end])
    quote
        datarun($esc_proj, $esc_funcname, $(esc_funcargs...))
    end
end

"""
    dataset_type(dataset)

Get a string representation of the "DataSet type", which represents the type of
the data *outside* Julia.

A given DataSet type may be mapped into many different Julia types. For example
consider the "Blob" type which is an array of bytes (commonly held in a file).
When loaded into Julia, this may be represented as a
    * IO             — via open())
    * String         — via open() |> read(_,String)
    * Vector{UInt8}  — via mmap)
    * Path
"""
function dataset_type(d::DataSet)
    # TODO: Enhance this once maps can be applied on top of the storage layer
    # Should we use MIME type? What about layering?
    d.storage["type"]
end

function _openall(func, opened, (dataset,T), to_open...)
    open(T, dataset) do newly_opened
        _openall(func, (opened..., newly_opened), to_open...)
    end
end

function _openall(func, opened)
    func(opened...)
end

# Match `dataset_type` of `ds` against `dispatch_table`, using the match to
# determine the appropriate Julia types we will open.
function _run(func, dispatch_table, ds::DataSet...)
    # For now, uses a simplistic exact matching strategy. We don't use Julia's
    # builtin dispatch here because
    # a) It seems wasteful to create a pile of tag types just for the purposes
    #    of matching some strings
    # b) It seems like a good idea to separate the declarative "data
    #    typesystem" (implicitly defined outside Julia) from Julia's type
    #    system and dispatch rules.
    dtypes = dataset_type.(ds)
    if !haskey(dispatch_table, dtypes)
        table = join(string.(keys(dispatch_table)), "\n")
        throw(ArgumentError("""No matching function $func for DataSet types $dtypes.

                            The types must match one of the following:
                            $table
                            """))
    end
    julia_types = dispatch_table[dtypes]
    to_open = Pair.(ds, julia_types)
    _openall(func, (), to_open...)
end

