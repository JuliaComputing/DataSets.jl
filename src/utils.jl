# Some basic utilities to validate "config-like" data
#
# (Perhaps these could be replaced with the use of JSON schema or some such?)

_key_match(config, (k,T)::Pair) = haskey(config, k) && config[k] isa T
_key_match(config, k::String) = haskey(config, k)

function _check_keys(config, context, keys)
    missed_keys = filter(k->!_key_match(config, k), keys)
    if !isempty(missed_keys)
        error("""
              Missing expected keys in $context:
              $missed_keys

              In DataSet fragment:
              $(sprint(TOML.print,config))
              """)
    end
end

struct VectorOf
    T
end

function _check_optional_keys(config, context, keys...)
    for (k, check) in keys
        if haskey(config, k)
            v = config[k] 
            if check isa Type && !(v isa check)
                error("""Invalid DataSet key $k. Expected type $check""")
            elseif check isa VectorOf && !(v isa AbstractVector &&
                                           all(x isa check.T for x in v))
                error("""Invalid DataSet key $k""")
            end
        end
    end
end

