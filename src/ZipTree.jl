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

struct File2
    root::ZipTreeRoot
    path::RelPath
end

Base.basename(file::File2) = basename(file.path)
Base.isdir(file::File2) = false
Base.isfile(file::File2) = true

function Base.show(io::IO, ::MIME"text/plain", file::File2)
    print(io, "📄 ", file.path, " @ ", file.root)
end

#-------------------------------------------------------------------------------
struct FileTree2 <: AbstractFileTree
    root::ZipTreeRoot
    path::RelPath
end

FileTree2(root::ZipTreeRoot) = FileTree2(root, RelPath())

Base.isfile(tree::FileTree2) = false

Base.basename(tree::FileTree2) = basename(tree.path)

function Base.getindex(tree::FileTree2, path::RelPath)
    newpath = joinpath(tree.path, path)
    i = findfirst(tree.root.file_info) do info
        info.path == newpath
    end
    if i == nothing
        error("Path $newpath doesn't exist in $tree")
    elseif tree.root.file_info[i].is_dir
        FileTree2(tree.root, newpath)
    else
        File2(tree.root, newpath)
    end
end

function Base.getindex(tree::FileTree2, name::AbstractString)
    getindex(tree, joinpath(RelPath(), name))
end

function _tree_children(tree::FileTree2)
    children = String[]
    for (i,info) in enumerate(tree.root.file_info)
        if dirname(info.path) == tree.path
            push!(children, basename(info.path))
        end
    end
    children
end

Base.IteratorSize(tree::FileTree2) = Base.SizeUnknown()
function Base.iterate(tree::FileTree2, state=nothing)
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

function Base.joinpath(tree::FileTree2, r::RelPath)
    # Should this AbsPath be rooted at `tree` rather than `tree.root`?
    AbsPath(tree.root, joinpath(tree.path, r))
end

function Base.joinpath(tree::FileTree2, s::AbstractString)
    AbsPath(tree.root, joinpath(tree.path, s))
end

# It appears that ZipFile.jl just doesn't have a way to rewind to the start of
# one of the embedded files.
# TODO: Upstream this
function _seek(io::ZipFile.ReadableFile, n::Integer)
    # Only support seeking to the start
    n == 0 || throw(ArguementError("Cannot efficiently seek zip stream to nonzero offset $n"))
    io._datapos = -1
    io._currentcrc32 = 0
    io._pos = 0
    io._zpos = 0
end

function Base.open(func::Function, f::File2; write=false, read=!write)
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

