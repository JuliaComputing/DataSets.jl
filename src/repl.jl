using REPL: LineEdit

# verbs
#
# add - Create a new association between data and 

# Make an Expr 
function make_data_repl_command(cmdstr)
    cmd_tokens = Base.shell_split(cmdstr)
    if cmd_tokens[1] == "new"
        quote
            new_dataset()
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
