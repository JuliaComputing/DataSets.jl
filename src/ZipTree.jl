using ZipFile

struct ZipPathInfo # Rename to ZipEntry ?
    is_dir::Bool
    path::RelPath
end

struct ZipTreeRoot
    reader::ZipFile.Reader
    file_info::Vector{ZipPathInfo} # same order as reader.files
end

function ZipTreeRoot(path::AbstractString)
    reader = ZipFile.Reader(path)
    ZipTreeRoot(reader, _zip_path_info.(reader.files))
end

Base.show(io::IO, root::ZipTreeRoot) = print(io, "ZipTreeRoot(...)")

function _zip_path_info(f)
    name = f.name
    is_dir = endswith(name, '/')
    name = is_dir ? name[1:end-1] : name
    path = DataSets.RelPath(split(name, '/'))
    ZipPathInfo(is_dir, path)
end


#-------------------------------------------------------------------------------
# FIXME: Factor back together with FileTree.jl !!

struct ZippedFileTree <: AbstractFileTree
    root::ZipTreeRoot
    path::RelPath
end

ZippedFileTree(root::ZipTreeRoot) = ZippedFileTree(root, RelPath())

Base.basename(tree::ZippedFileTree) = basename(tree.path)

function Base.getindex(tree::ZippedFileTree, path::RelPath)
    newpath = joinpath(tree.path, path)
    i = findfirst(tree.root.file_info) do info
        info.path == newpath
    end
    if i == nothing
        error("Path $newpath doesn't exist in $tree")
    elseif tree.root.file_info[i].is_dir
        ZippedFileTree(tree.root, newpath)
    else
        File(tree.root, newpath)
    end
end

function Base.getindex(tree::ZippedFileTree, name::AbstractString)
    getindex(tree, joinpath(RelPath(), name))
end

function _tree_children(tree::ZippedFileTree)
    children = String[]
    for (i,info) in enumerate(tree.root.file_info)
        if dirname(info.path) == tree.path
            push!(children, basename(info.path))
        end
    end
    children
end

Base.IteratorSize(tree::ZippedFileTree) = Base.SizeUnknown()
function Base.iterate(tree::ZippedFileTree, state=nothing)
    if state == nothing
        children = _tree_children(tree)
        itr = iterate(children)
    else
        (children, cstate) = state
        itr = iterate(children, cstate)
    end
    if itr == nothing
        return nothing
    else
        (name, cstate) = itr
        (tree[name], (children, cstate))
    end
end

function Base.joinpath(tree::ZippedFileTree, r::RelPath)
    # Should this AbsPath be rooted at `tree` rather than `tree.root`?
    AbsPath(tree.root, joinpath(tree.path, r))
end

function Base.joinpath(tree::ZippedFileTree, s::AbstractString)
    AbsPath(tree.root, joinpath(tree.path, s))
end

function Base.open(func::Function, f::File{ZipTreeRoot}; write=false, read=!write)
    if write
        error("Error writing file at read-only path $f")
    end
    i = findfirst(info->info.path == f.path, f.root.file_info)
    io = f.root.reader.files[i]
    _seek(io, 0)
    func(io)
end

#=
w = ZipFile.Writer("/tmp/example.zip");
f = ZipFile.addfile(w, "hello.txt");
write(f, "hello world!\n");
f = ZipFile.addfile(w, "julia.txt", method=ZipFile.Deflate);
write(f, "Julia\n"^5);
close(w)
r = ZipFile.Reader("/tmp/example.zip");
for f in r.files
   println("Filename: $(f.name)")
   write(stdout, read(f, String));
end
close(r)
=#



#-------------------------------------------------------------------------------

# Fixes for ZipFile.jl
# TODO: Upstream these!

# It appears that ZipFile.jl just doesn't have a way to rewind to the start of
# one of the embedded files.
function _seek(io::ZipFile.ReadableFile, n::Integer)
    # Only support seeking to the start
    n == 0 || throw(ArguementError("Cannot efficiently seek zip stream to nonzero offset $n"))
    io._datapos = -1
    io._currentcrc32 = 0
    io._pos = 0
    io._zpos = 0
end

# Needed for use as `src` in `write(dst::IO, src::IO)`.
Base.readavailable(io::ZipFile.ReadableFile) = read(io)
