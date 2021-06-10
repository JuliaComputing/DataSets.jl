module Repl

using Markdown

_data_repl_help = md"""
## DataSets Data REPL

|   Command    | Alias | Action |
|:----------   |:----- | :---------- |
| `list`       | `ls`  | List all datasets by name |
| `show $name` |       | Show the content of dataset `$name` |
| `help`      | `?` | Show this message |
| `addproject` |       | Add a data project to the current stack |

"""

using ..DataSets

using ResourceContexts

using REPL
using ReplMaker
# using URIs

# hex dump in xxd format
function hexdump(out_stream, buf; groups_per_line=8, group_size=2)
    linesize = groups_per_line*group_size
    for line = 1:div(length(buf), linesize, RoundUp)
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

struct DataCompletionProvider <: REPL.LineEdit.CompletionProvider
end

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
    tokens = split(full, r" +", keepempty=true)
    if length(tokens) == 1
        # Completions for basic commands
        completions = String[]
        for cmdset in [("list","ls"), ("show",), ("addproject",)]
            for cmd in cmdset
                if cmd == tokens[1]
                    # Space after full length command
                    return ([" "], "", true)
                end
                if startswith(cmd, tokens[1])
                    push!(completions, cmd*" ")
                    break
                end
            end
        end
        return (completions, tokens[1], !isempty(completions))
    end
    cmd = popfirst!(tokens)
    if cmd == "show" && length(tokens) <= 1
        tok_prefix = isempty(tokens) ? "" : tokens[1]
        completions = String[]
        ks = sort!(collect(keys(DataSets.PROJECT)))
        for k in ks
            if startswith(k, tok_prefix) && k != tok_prefix
                push!(completions, k)
            end
        end
        return (completions, tok_prefix, !isempty(completions))
    end
    return ([], "", false)
end

# Translate `data>` REPL syntax into an Expr to be evaluated in the REPL
# backend.
function make_data_repl_command(cmdstr)
    # Use shell tokenization rules for familiarity
    tokens = Base.shell_split(cmdstr)
    cmd = tokens[1]
    popfirst!(tokens)
    if cmd in ("ls", "list")
        return quote
            # Will be `show()`n by the REPL
            DataSets.PROJECT
        end
    elseif cmd == "addproject"
        path = tokens[1]
        return quote
            proj = $DataSets.TomlFileDataProject($path)
            pushfirst!($DataSets.PROJECT, proj)
            proj
        end
    elseif cmd == "show"
        name = tokens[1]
        return quote
            $Repl.show_dataset($name)
        end
    elseif cmd in ("help", "?")
        return _data_repl_help
    else
        error("Invalid data REPL syntax: \"$cmdstr\"")
    end
end

function show_dataset(name)
    out_stream = stdout
    @context begin
        data = @! open(dataset(name))
        _show_dataset(out_stream, data)
    end
end

function _show_dataset(out_stream::IO, blob::Blob)
    @context begin
        io = @! open(IO, blob)
        N = 1024
        buf = read(io, N)
        str = String(copy(buf))
        n_textlike = count(str) do c
            isvalid(c) || return false
            isprint(c) || c in ('\n', '\r', '\t')
        end
        if n_textlike / length(str) > 0.95
            # It's approximately UTF-8 encoded text data.
            print(out_stream, str)
        else
            # It's something else, perhaps binary or another text
            # encoding. Do a hex dump instead.
            hexdump(out_stream, buf)
        end
        if !eof(io)
            println(out_stream, "…")
        end
    end
end

function _show_dataset(out_stream::IO, tree::BlobTree)
    show(out_stream, MIME("text/plain"), tree)
end

function _show_dataset(out_stream::IO, x)
    show(out_stream, MIME("text/plain"), x)
end

function init_data_repl(; start_key = ">")
    ReplMaker.initrepl(make_data_repl_command,
                       repl = Base.active_repl,
                       # valid_input_checker = Support for multiple lines syntax?
                       prompt_text = "data> ",
                       prompt_color = :red,
                       start_key = start_key,
                       sticky_mode=true,
                       mode_name = "Data_Manager",
                       completion_provider = DataCompletionProvider(),
                       startup_text=true)
    nothing
end

function __init__()
    isinteractive() && init_data_repl()
end

end
