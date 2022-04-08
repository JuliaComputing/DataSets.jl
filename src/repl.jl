module DataREPL

using Markdown

_data_repl_help = md"""
## DataSets Data REPL

Press `>` to enter the data repl. Press TAB to complete commands.

### Informational commands

|   Command    | Alias     | Action      |
|:----------   |:--------- | :---------- |
| `help`       | `?`       | Show this message |
| `list`       | `ls`      | List stack of projects and datasets by name |
| `show $name` |           | Preview the content of dataset `$name` |
| `stack` | `st`           | Manipulate the global data search stack |
| `stack push $path` | `st push` | Add data project `$path` to front of the search stack |
| `stack pop`  | `st pop`  | Remove data project from front of the search stack    |

### Commands for creating and deleting datasets

|   Command    | Alias     | Action      |
|:----------   |:--------- | :---------- |
| `copy`       | `cp`      | Create a new dataset by copying from another dataset or from local disk |
| `create`     |           | Create a new dataset |
| `delete`     | `rm`      | Remove a named dataset |

"""

using ..DataSets

using ResourceContexts

using REPL
using ReplMaker
# using URIs

#-------------------------------------------------------------------------------
# Utilities for browsing dataset content
function _show_dataset(name)
    out_stream = stdout
    @context begin
        data = @! open(dataset(name))
        show_dataset(out_stream, data)
    end
end

# hex dump in xxd format
function hexdump(out_stream, buf; groups_per_line=8, group_size=2, max_lines=typemax(Int))
    linesize = groups_per_line*group_size
    for line = 1:min(max_lines, div(length(buf), linesize, RoundUp))
        linebuf = buf[(line-1)*linesize+1 : min(line*linesize,end)]
        address = (line-1)*linesize
        print(out_stream, string(address, base=16, pad=4), ": ")
        for group = 1:groups_per_line
            for i=1:group_size
                j = (group-1)*group_size+i
                if j <= length(linebuf)
                    print(out_stream, string(linebuf[j], base=16, pad=2))
                else
                    print(out_stream, "  ")
                end
            end
            print(out_stream, ' ')
        end
        print(out_stream, ' ')
        for j = 1:linesize
            c = Char(j <= length(linebuf) ? linebuf[j] : ' ')
            print(out_stream, isprint(c) ? c : '.')
        end
        print(out_stream, '\n')
    end
end

function show_dataset(out_stream::IO, blob::Blob)
    @context begin
        io = @! open(IO, blob)
        N = 1024
        buf = read(io, N)
        str = String(copy(buf))
        n_textlike = count(str) do c
            isvalid(c) || return false
            isprint(c) || c in ('\n', '\r', '\t')
        end
        display_lines, _ = displaysize(out_stream)
        max_lines = max(5, display_lines ÷ 2)
        if length(str) == 0 || n_textlike / length(str) > 0.95
            # It's approximately UTF-8 encoded text data - print as text
            lines = split(str, '\n', keepempty=true)
            nlines = min(lastindex(lines), max_lines)
            print(out_stream, join(lines[1:nlines], '\n'))
            println(out_stream)
            if !eof(io) || nlines < length(lines)
                println(out_stream, "⋮")
            end
        else
            # It's something else, perhaps binary or another text
            # encoding. Do a hex dump instead.
            println(out_stream, "Binary data:")
            hexdump(out_stream, buf; max_lines=max_lines)
            if !eof(io)
                println(out_stream, "⋮")
            end
        end
    end
end

function show_dataset(out_stream::IO, tree::BlobTree)
    show(out_stream, MIME("text/plain"), tree)
end

function show_dataset(out_stream::IO, x)
    show(out_stream, MIME("text/plain"), x)
end

function _copy(source_name, dest_name)
    source =
        haskey(DataSets.PROJECT, source_name) ? dataset(source_name)            :
        ispath(source_name)                   ? DataSets.from_path(source_name) :
        error("\"$source_name\" must be either a dataset name or a local path")

    DataSets.create(dest_name, source=source)
end

#-------------------------------------------------------------------------------
# REPL command handling and completions

# function split_command(str)
#     token_ranges = []
#     i = 1
#     while true
#         rng = findnext(r"[^\s]+", full, i)
#         !isnothing(rng) || break
#         push!(token_ranges, rng)
#         i = last(rng)+1
#     end
#     tokens = getindex.(full, token_ranges)
#     token_ranges, tokens
# end

function complete_command_list(cmd_prefix, commands)
    # Completions for basic commands
    completions = String[]
    for cmdset in commands
        for cmd in cmdset
            if startswith(cmd, cmd_prefix)
                push!(completions, cmd*" ")
                break
            end
        end
    end
    return completions
end

function path_str(path_completion)
    path = REPL.REPLCompletions.completion_text(path_completion)
    if Sys.iswindows()
        # On windows, REPLCompletions.complete_path() adds extra escapes for
        # use within a normal string in the Juila REPL but we don't need those.
        path = replace(path, "\\\\"=>'\\')
    end
    return path
end

function complete(str_to_complete)
    tokens = split(str_to_complete, r" +", keepempty=true)
    cmd = popfirst!(tokens)
    if isempty(tokens)
        # Completions for basic commands
        completions = complete_command_list(cmd, [
            ("list","ls"),
            ("delete","rm"),
            ("copy","cp"),
            ("create",),
            ("show",),
            ("stack",),
            ("help","?")
        ])
        # Empty completion => return anyway to show user their prefix is wrong
        return (completions, cmd, !isempty(completions))
    end
    completion_types = []
    if cmd in ("show", "rm", "delete") && length(tokens) == 1
        push!(completion_types, :dataset_name)
    elseif cmd in ("copy", "cp") && length(tokens) <= 1
        push!(completion_types, :dataset_name)
        push!(completion_types, :path)
    elseif cmd == "stack"
        if length(tokens) <= 1
            subcmd_prefix = isempty(tokens) ? "" : tokens[1]
            # Completions for project stack subcommands
            completions = complete_command_list(subcmd_prefix, [
                ("push",),
                ("pop",),
            ])
            return (completions, tokens[1], !isempty(completions))
        else
            subcmd = popfirst!(tokens)
            if subcmd == "push" && length(tokens) <= 1
                push!(completion_types, :path)
            end
        end
    end

    completions = String[]
    prefix = isempty(tokens) ? "" : last(tokens)
    if :dataset_name in completion_types
        # Completion on DataSet names
        ks = sort!(collect(keys(DataSets.PROJECT)))
        for k in ks
            if startswith(k, prefix) && k != prefix
                push!(completions, k)
            end
        end
    end
    if isempty(completions) && :path in completion_types
        # Completion on filesystem paths
        (path_completions, range, should_complete) =
            REPL.REPLCompletions.complete_path(prefix, length(prefix))
        p = isempty(range) ? prefix : prefix[1:prevind(prefix, first(range))]
        append!(completions, p*path_str(c) for c in path_completions)
    end
    return (completions, prefix, !isempty(completions))
end

# Translate `data>` REPL syntax into an Expr to be evaluated in the REPL
# backend.
function parse_data_repl_cmd(cmdstr)
    # Use shell tokenization rules for familiarity
    tokens = Base.shell_split(cmdstr)
    cmd = tokens[1]
    popfirst!(tokens)
    if cmd in ("list", "ls")
        return quote
            $DataSets.PROJECT
        end
    elseif cmd == "stack" && length(tokens) >= 1
        subcmd = popfirst!(tokens)
        if subcmd == "push"
            path = popfirst!(tokens)
            return quote
                proj = $DataSets.data_project_from_path($path)
                stack = $DataSets.PROJECT
                pushfirst!(stack, proj)
                stack
            end
        elseif subcmd == "pop"
            return quote
                stack = $DataSets.PROJECT
                popfirst!(stack)
                stack
            end
        end
    elseif cmd == "show"
        name = tokens[1]
        return quote
            $DataREPL._show_dataset($name)
        end
    elseif cmd == "create" && length(tokens) == 2
        type = popfirst!(tokens)
        name = popfirst!(tokens)
        return quote
            $DataSets.create($name, dtype=$type)
        end
    elseif cmd in ("copy", "cp") && length(tokens) == 2
        source_name = popfirst!(tokens)
        dest_name = popfirst!(tokens)
        return quote
            $DataREPL._copy($source_name, $dest_name)
        end
    elseif cmd in ("delete", "rm") && length(tokens) == 1
        name = tokens[1]
        return quote
            $DataSets.delete($name)
        end
    elseif cmd in ("help", "?")
        return _data_repl_help
    end
    error("Invalid data REPL syntax: \"$cmdstr\"")
end


#-------------------------------------------------------------------------------
# Integration with REPL / ReplMaker
struct DataCompletionProvider <: REPL.LineEdit.CompletionProvider
end

function REPL.complete_line(provider::DataCompletionProvider,
                            state::REPL.LineEdit.PromptState)::
                            Tuple{Vector{String},String,Bool}
    # See REPL.jl complete_line(c::REPLCompletionProvider, s::PromptState)
    partial = REPL.beforecursor(state.input_buffer)
    full = REPL.LineEdit.input_string(state)
    if partial != full
        # For now, only complete at end of line
        return ([], "", false)
    end
    complete(full)
end

function init_data_repl(; start_key = ">")
    ReplMaker.initrepl(parse_data_repl_cmd,
                       repl = Base.active_repl,
                       # valid_input_checker = Support for multiple lines syntax?
                       prompt_text = "data> ",
                       prompt_color = :red,
                       start_key = start_key,
                       sticky_mode = true,
                       mode_name = "DataSets",
                       completion_provider = DataCompletionProvider(),
                       startup_text = false)
    nothing
end

function __init__()
    isinteractive() && init_data_repl()
end

end
