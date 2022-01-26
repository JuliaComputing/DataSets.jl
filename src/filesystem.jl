#
# Storage Driver implementation for trees which are rooted in the file system
# (in git terminology, there exists a "working copy")
#
abstract type AbstractFileSystemRoot end

# These functions sys_abspath and sys_joinpath generate/joins OS-specific
# _local filesystem paths_ out of logical paths. They should be defined only
# for trees which are rooted in the actual filesystem.
function sys_abspath(root::AbstractFileSystemRoot, path::RelPath)
    rootpath = sys_abspath(root)
    return isempty(path.components) ? rootpath : joinpath(rootpath, sys_joinpath(path))
end

sys_joinpath(path::RelPath) = isempty(path.components) ? "" : joinpath(path.components...)
sys_abspath(path::AbsPath) = sys_abspath(path.root, path.path)
sys_abspath(tree::BlobTree) = sys_abspath(tree.root, tree.path)
sys_abspath(file::Blob) = sys_abspath(file.root, file.path)

#--------------------------------------------------
# Storage data interface for trees
#
# TODO: Formalize this interface!

## 1. Query

# TODO: would it be better to express the following dispatch in terms of
# AbsPath{<:AbstractFileSystemRoot} rather than usin double dispatch?

Base.isdir(root::AbstractFileSystemRoot, path::RelPath) = isdir(sys_abspath(root, path))
Base.isfile(root::AbstractFileSystemRoot, path::RelPath) = isfile(sys_abspath(root, path))
Base.ispath(root::AbstractFileSystemRoot, path::RelPath) = ispath(sys_abspath(root, path))

Base.summary(io::IO, root::AbstractFileSystemRoot) = print(io, sys_abspath(root))

Base.readdir(root::AbstractFileSystemRoot, path::RelPath) = readdir(sys_abspath(root, path))

## 2. Mutation
#
# TODO: Likely requires rework!

function Base.mkdir(root::AbstractFileSystemRoot, path::RelPath; kws...)
    if !iswriteable(root)
        error("Cannot make directory in read-only tree root at $(sys_abspath(p.root))")
    end
    mkdir(sys_abspath(root, path), args...)
    return BlobTree(root, path)
end

function Base.rm(root::AbstractFileSystemRoot, path::RelPath; kws...)
    rm(sys_abspath(root,path); kws...)
end

#--------------------------------------------------
# Storage data interface for Blob

# TODO: Make this the generic implementation for AbstractDataStorage
function Base.open(f::Function, as_type::Type{IO},
                   root::AbstractFileSystemRoot, path; kws...)
    @context f(@! open(as_type, root, path; kws...))
end

@! function Base.open(::Type{IO}, root::AbstractFileSystemRoot, path;
                      write=false, read=!write, kws...)
    if !iswriteable(root) && write
        error("Error writing file at read-only path $path")
    end
    @! open(sys_abspath(root, path); read=read, write=write, kws...)
end

Base.read(root::AbstractFileSystemRoot, path::RelPath, ::Type{T}) where {T} =
    read(sys_abspath(root, path), T)
Base.read(root::AbstractFileSystemRoot, path::RelPath) =
    read(sys_abspath(root, path))

#--------------------------------------------------
"""

## Metadata spec

For Blob:
```
    [datasets.storage]
    driver="FileSystem"
    type="Blob"
    path=\$(path_to_file)
```

For BlobTree:
```
    [datasets.storage]
    driver="FileSystem"
    type="BlobTree"
    path=\$(path_to_directory)
```
"""
struct FileSystemRoot <: AbstractFileSystemRoot
    path::String
    read::Bool
    write::Bool
end

function FileSystemRoot(path::AbstractString; write=false, read=true)
    path = abspath(path)
    FileSystemRoot(path, read, write)
end

iswriteable(root::FileSystemRoot) = root.write

sys_abspath(root::FileSystemRoot) = root.path

function Base.abspath(relpath::RelPath)
    Base.depwarn("""
        `abspath(::RelPath)` defaults to using `pwd()` as the root of the path
        but this leads to fragile code so will be removed in the future""",
        :abspath)
    AbsPath(FileSystemRoot(pwd(); write=true, read=true), relpath)
end

#-------------------------------------------------------------------------------
# Infrastructure for a somewhat more functional interface for creating file
# trees than the fully mutable version we usually use.

mutable struct TempFilesystemRoot <: AbstractFileSystemRoot
    path::Union{Nothing,String}
    function TempFilesystemRoot(path)
        root = new(path)
        finalizer(root) do r
            if !isnothing(r.path)
                rm(r.path, recursive=true, force=true)
            end
        end
        return root
    end
end

function Base.readdir(root::TempFilesystemRoot, path::RelPath)
    return isnothing(root.path) ? [] : readdir(sys_abspath(root, path))
end

iswriteable(root::TempFilesystemRoot) = true
sys_abspath(root::TempFilesystemRoot) = root.path

"""
    newdir()

Create a new temporary `BlobTree` which can have files assigned into it and may
be assigned to a permanent location in a persistent `BlobTree`. If not assigned
to a permanent location, the temporary tree is cleaned up during garbage
collection.
"""
function newdir(ctx::AbstractFileSystemRoot=FileSystemRoot(tempdir(), write=true))
    # cleanup=false: we manage our own cleanup via the finalizer
    path = mktempdir(sys_abspath(ctx), cleanup=false)
    return BlobTree(TempFilesystemRoot(path))
end
newdir(ctx::BlobTree) = newdir(ctx.root)

function newfile(ctx::AbstractFileSystemRoot=FileSystemRoot(tempdir(), write=true))
    path, io = mktemp(sys_abspath(ctx), cleanup=false)
    close(io)
    return Blob(TempFilesystemRoot(path))
end
newfile(ctx::BlobTree) = newfile(ctx.root)

"""
    newfile(func)
    newfile(func, ctx)

Create a new temporary `Blob` object which may be later assigned to a permanent
location in a `BlobTree`. If not assigned to a permanent location, the
temporary file is cleaned up during garbage collection.

# Example

```
tree[path"some/demo/path.txt"] = newfile() do io
    println(io, "Hi there!")
end
```
"""
function newfile(f::Function, ctx=FileSystemRoot(tempdir(), write=true))
    path, io = mktemp(sys_abspath(ctx), cleanup=false)
    try
        f(io)
    catch
        rm(path)
        rethrow()
    finally
        close(io)
    end
    return Blob(TempFilesystemRoot(path))
end

# Move srcpath to destpath, making all attempts to preserve the original
# content of `destpath` if anything goes wrong. We assume that `srcpath` is
# temporary content which doesn't need to be protected.
function mv_force_with_dest_rollback(srcpath, destpath, tempdir_parent)
    holding_area = nothing
    held_path = nothing
    if ispath(destpath)
        # If the destination path exists, improve the atomic nature of the
        # update by first moving existing data to a temporary directory.
        holding_area = mktempdir(tempdir_parent, prefix="jl_to_remove_", cleanup=false)
        name = basename(destpath)
        held_path = joinpath(holding_area,name)
        mv(destpath, held_path)
    end
    try
        mv(srcpath, destpath)
    catch
        try
            if !isnothing(holding_area)
                # Attempt to put things back as they were!
                mv(held_path, destpath)
            end
        catch
            # At this point we've tried our best to preserve the user's data
            # but something has gone wrong, likely at the OS level. The user
            # will have to clean up manually if possible.
            error("""
                  Something when wrong while moving data to path $destpath.

                  We tried restoring the original data to $destpath, but were
                  met with another error. The original data is preserved in
                  $held_path

                  See the catch stack for the root cause.
                  """)
        end
        rethrow()
    end
    if !isnothing(holding_area)
        # If we get to here, it's safe to remove the holding area
        rm(holding_area, recursive=true)
    end
end

function Base.setindex!(tree::BlobTree{<:AbstractFileSystemRoot},
                        tmpdata::Union{Blob{TempFilesystemRoot},BlobTree{TempFilesystemRoot}},
                        name::AbstractString)
    if !iswriteable(tree.root)
        error("Attempt to move to a read-only tree $tree")
    end
    if isnothing(tmpdata.root.path)
        type = isdir(tmpdata) ? "directory" : "file"
        error("Attempted to root a temporary $type which has already been moved to $(tree.path)/$name ")
    end
    if !isempty(tree.path)
        # Eh, the number of ways the user can misuse this isn't really funny :-/
        error("Temporary trees must be moved in full. The tree had non-empty path $(tree.path)")
    end
    destpath = sys_abspath(joinpath(tree, name))
    srcpath = sys_abspath(tmpdata)
    tempdir_parent = sys_abspath(tree)
    mv_force_with_dest_rollback(srcpath, destpath, tempdir_parent)
    # Transfer ownership of the data to `tree`. This is ugly to be sure, as it
    # leaves `tmpdata` empty! However, we'll have to live with this wart unless
    # we want to be duplicating large amounts of data on disk.
    tmpdata.root.path = nothing
    return tree
end

# It's interesting to read about the linux VFS interface in regards to how the
# OS actually represents these things. For example
# https://stackoverflow.com/questions/36144807/why-does-linux-use-getdents-on-directories-instead-of-read




#--------------------------------------------------

# Filesystem storage driver
function connect_filesystem(f, config, dataset)
    path = config["path"]
    type = config["type"]
    if type == "Blob"
        isfile(path) || throw(ArgumentError("$(repr(path)) should be a file"))
        storage = Blob(FileSystemRoot(path))
    elseif type == "BlobTree"
        isdir(path)  || throw(ArgumentError("$(repr(path)) should be a directory"))
        storage = BlobTree(FileSystemRoot(path))
        path = dataspec_fragment_as_path(dataset)
        if !isnothing(path)
            storage = storage[path]
        end
    else
        throw(ArgumentError("DataSet type $type not supported on the filesystem"))
    end
    f(storage)
end
add_storage_driver("FileSystem"=>connect_filesystem)

