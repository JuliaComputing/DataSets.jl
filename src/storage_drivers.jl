# Global record of registered storage drivers

const _storage_drivers_lock = ReentrantLock()
const _storage_drivers = Dict{String,Any}()

"""
    add_storage_driver(driver_name=>storage_opener)

Associate DataSet storage driver named `driver_name` with `storage_opener`.
When a `dataset` with `storage.driver == driver_name` is opened,
`storage_opener(user_func, storage_config, dataset)` will be called. Any
existing storage driver registered to `driver_name` will be overwritten.

As a matter of convention, `storage_opener` should generally take configuration
from `storage_config` which is just `dataset.storage`. But to avoid config
duplication it may also use the content of `dataset`, (for example, dataset.uuid).

Packages which define new storage drivers should generally call
`add_storage_driver()` within their `__init__()` functions.
"""
function add_storage_driver((name,opener)::Pair)
    lock(_storage_drivers_lock) do
        _storage_drivers[name] = opener
    end
end

function add_storage_driver(project::AbstractDataProject)
    for conf in data_drivers(project)
        pkgid = PkgId(UUID(conf["module"]["uuid"]), conf["module"]["name"])
        if Base.haskey(Base.package_locks, pkgid)
            # Hack: Avoid triggering another call to require() for packages
            # which are already in the process of being loaded. (This would
            # result in a deadlock!)
            #
            # Obviously this depends on Base internals...
            continue
        end
        mod = Base.require(pkgid)
        #=
        # TODO: Improve driver loading invariants.
        #
        # The difficulty here is that there's two possible ways for drivers to
        # work:
        # 1. The driver depends explicitly on `using DataSets`, so
        #    DataSets.__init__ is called *before* the Driver.__init__.
        # 2. The driver uses a Requires-like mechanism to support multiple
        #    incompatible DataSets versions, so Driver.__init__ can occur
        #    *before* DataSets.__init__.
        #
        # This makes it hard for DataSets to check which drivers are added by a
        # module: In case (2), the following check fails when the driver is
        # loaded before DataSets and in case (1) we hit the double-require
        # problem, resulting in the Base.package_locks bailout which disables
        # the check below.
        #
        if conf["type"] == "storage"
            driver_name = conf["name"]
            # `mod` is assumed to run add_storage_driver() inside its __init__,
            # unless the symbol mod.datasets_load_hook exists (in which case we
            # call this instead).
            lock(_storage_drivers_lock) do
                get(_storage_drivers, driver_name) do
                    error("Package $pkgid did not provide storage driver $driver_name")
                end
            end
        end
        =#
    end
end

function _find_driver(dataset)
    storage_config = dataset.storage
    driver_name = get(storage_config, "driver") do
        error("`storage.driver` configuration not found for dataset $(dataset.name)")
    end
    driver = lock(_storage_drivers_lock) do
        get(_storage_drivers, driver_name) do
            error("""
                  Storage driver $(repr(driver_name)) not found for dataset $(dataset.name).
                  Current drivers are $(collect(keys(_storage_drivers)))
                  """)
        end
    end
end

