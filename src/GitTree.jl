struct GitTreeRoot
    path::String
    read::Bool
    write::Bool
end

function GitTreeRoot(path::AbstractString; write=false, read=true)
    path = abspath(path)
    if !isdir(path)
        throw(ArgumentError("$(repr(path)) must be a directory"))
    end
    GitTreeRoot(path, read, write)
end

sys_abspath(root::GitTreeRoot) = root.path
sys_abspath(ap::AbsPath{GitTreeRoot}) = joinpath(sys_abspath(ap.root), sys_joinpath(ap.path))

function Base.open(f::Function, root::GitTreeRoot)
    git(subcmd) = setenv(`git $subcmd`, dir=root.path)
    s = read(git(`status --porcelain`), String)
    isempty(s) || error("Git working copy is dirty")
    result = f(BlobTree(root))
    # FIXME: From the point of view of this code, it seems unnatural to attach
    # `write` to GitTreeRoot.
    if root.write
        run(pipeline(git(`add $(root.path)`), stdout=devnull))
        run(pipeline(git(`commit --allow-empty $(root.path) -m "New version"`), stdout=devnull))
    end
    result
end

#-------------------------------------------------------------------------------
# FIXME: Factor together with BlobTreeRoot

function Base.haskey(tree::BlobTree{GitTreeRoot}, name::AbstractString)
    ispath(sys_abspath(joinpath(tree,name)))
end

function Base.open(func::Function, f::Blob{GitTreeRoot}; write=false, read=!write)
    if !f.root.write && write
        error("Error writing file at read-only path $f")
    end
    open(func, sys_abspath(f); read=read, write=write)
end

function Base.open(func::Function, p::AbsPath{GitTreeRoot}; write=false, read=!write)
    if !p.root.write && write
        error("Error writing file at read-only path $p")
    end
    open(func, sys_abspath(p); read=read, write=write)
end

function Base.mkdir(p::AbsPath{GitTreeRoot}, args...)
    if !p.root.write
        error("Cannot make directory in read-only tree root at $(sys_abspath(p.root))")
    end
    mkdir(sys_abspath(p), args...)
    return BlobTree(p.root, p.path)
end

