"""
    `DataSets.DataApp` contains all `DataSets` "application level" code and state.

This includes the builtin REPL utilities and the default data project for use
within a julia session.

In contrast, the main `DataSets` module includes only core data structures and
operations which don't depend on global state.
"""
module DataApp

using ..DataSets
import ..DataSets: DataSet, DataProject, link_dataset

using REPL: LineEdit
using URIParser
using ReplMaker

# Global state for Julia session

_current_project = DataProject()

# Is allowing this global default good or bad?
DataSets.dataset(name) = dataset(_current_project, name)

# Possible REPL verbs
#
# Adding
#   add link - Create a new association between external data and the project
#   add new  - Add a new dataset *within* the project
#
# Removing
#   rm [link]   - Unlink the dataset from the project
#   rm ! <name> - Remove the dataset from the project, and remove the actual
#                 data too if it's embedded.
#
# open - REPL data viewer (?)
# list - 

# Translate `data>` REPL syntax into an Expr to be evaluated in the REPL
# backend.
function make_data_repl_command(cmdstr)
    # Use shell tokenization rules for familiarity
    cmd_tokens = Base.shell_split(cmdstr)
    cmdname = cmd_tokens[1]
    if cmdname in ("ln", "link")
        # FIXME: Test :incomplete
        if length(cmd_tokens) < 3
            return Expr(:incomplete, "Needs name and location")
        end
        name = cmd_tokens[2]
        location = cmd_tokens[3]
        toks = cmd_tokens[4:end]
        if any(toks[1:2:end] .!= "|")
            error("Expected '|' separated decoders after $location. Got $toks.")
        end
        decoders = toks[2:2:end]
        return quote
            name = $name
            location = DataSets.DataApp.expand_location($location)
            decoders = DataSets.DataApp.expand_decoder.($decoders)
            d = DataSets.DataSet(default_name=name, location=location, decoders=decoders)
            DataSets.link_dataset(DataSets.DataApp._current_project, name=>d)
            d
        end
    elseif cmdname == "unlink"
        name = cmd_tokens[2]
        return quote
            DataSets.unlink_dataset(DataSets.DataApp._current_project, $name)
            nothing
        end
    elseif cmdname in ("ls", "list")
        return quote
            # Will be `show()`n by the REPL
            DataSets.DataApp._current_project
        end
    else
        error("Invalid data REPL syntax: \"$cmdstr\"")
    end
end

function init_repl(; start_key = ">")
    ReplMaker.initrepl(make_data_repl_command,
                       repl = Base.active_repl,
                       # valid_input_checker = Support for multiple lines syntax?
                       prompt_text = "data> ",
                       prompt_color = :red,
                       start_key = start_key,
                       sticky_mode=true,
                       mode_name = "Data_Manager")
    nothing
end

function repl_link_dataset(name, accessors)
    @assert length(accessors) == 1
    uri = URIParser.parse_url(accessors[1])
    d = DataSet(name=name, location=uri)
    link_dataset(d)
end

function list_datasets(io, proj::DataProject)
    for (name,d) in proj.datasets
        println(io, name, " @ ", d.location)
    end
end

function expand_location(location)
    path = abspath(location)
    if ispath(path)
        uri = URIParser.URI("file", "", 0, path)
    else
        uri = URIParser.parse_url(location)
    end
end

function expand_decoder(decoder)
    # TODO: expand the short REPL syntax into decoder objects?
    decoder
end

end
