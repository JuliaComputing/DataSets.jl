### A Pluto.jl notebook ###
# v0.11.13

using Markdown
using InteractiveUtils

# ╔═╡ 794887e2-f240-11ea-3f51-2d44e98e181c
begin
	using Revise
    using DataSets
	using HTTP.URIs
end

# ╔═╡ 48da7d86-f240-11ea-1627-a33e327d3043
md"""
# DataSets.jl

* Decouple data infrastructure and format from processing code; avoid command line and data wrangling wrappers.
* Relocate computation and data; from laptop to JuliaHub to HPC system, etc.
"""

# ╔═╡ 5a7f286a-f269-11ea-12a6-f9ff81348191
md"""
# What is a DataSet?

`DataSet` is metadata describing "any" data store + the code to load and store to it. Declarative.

Examples of storage
* Filesystem, S3, Git server
* Research data management servers
* Relational databases

Types of data
* Path-indexed tree-like data (Filesystem, Git, S3, Zip, HDF5, ...)
* Blobs (1D arrays of bytes)
* Arrays in general (eg, large geospatial rasters)
* Tables
"""

# ╔═╡ 62cfc2c6-f26f-11ea-031d-f3a16062e3e8
md"""
# Tree-indexed data

Tackle this as a first use case, as a "hieracical tree of blobs" is so common.

(But we don't have a well-defined path tree abstraction which already exists!)
"""

# ╔═╡ ab03a5aa-f26d-11ea-29fa-d5b93e2abd54
d = DataSet(default_name="SomeTree",
			location=URI("file://xyz/home/chris/.julia/dev/DataSets/scratch/foo"),
			decoders=["tree"])

# ╔═╡ 1dbc4580-f26d-11ea-09e9-6b26b583cca2
t = open(d)

# ╔═╡ cb5510fa-f26d-11ea-108d-f11660a2288c
showtree(t)

# ╔═╡ ac4da728-f270-11ea-0c79-cb2691e3845c
md"""
## Zipped trees

Another example; a zipped file as a tree with the same API
"""

# ╔═╡ 588ee8d4-f270-11ea-22f0-514b725b3c1e
dz = DataSet(default_name="Foo",
 			 location=URI("file://xyz/home/chris/.julia/dev/DataSets/scratch/foo.zip"),
 			 decoders=["file","zip"])

# ╔═╡ d07dca2c-f270-11ea-25b8-0756b5e518db
tz = open(dz)

# ╔═╡ 6c2147f4-f270-11ea-2378-e74edb8bae91
showtree(tz)

# ╔═╡ 76dfef78-f249-11ea-00ca-f32459993b38
md"""
# Data Lifecycle

AKA `open` and `close` for data. Why is this important?

It provides events so we can
* Create and commit versions
* Create and record provenance information
* Update metadata, such as timestamps (eg for loosely coupled dataflows)
"""

# ╔═╡ 0a9e0862-f402-11ea-2b96-533289dcb156
md"""
## Example: Git versions
"""

# ╔═╡ 565883ee-f26e-11ea-31ec-eb89f6019af7
function append_tree(tree)
    j = 1
    dname(k) = "d$(string(k, pad=2))"
    while haskey(tree, dname(j))
        j += 1
    end
    for i in j:j+4
        dir = mkdir(joinpath(tree, dname(i)))
        open(joinpath(dir, "a.txt"), write=true) do io
            println(io, "The content of $i a.txt")
        end
        open(joinpath(dir, "b.txt"), write=true) do io
            println(io, "The content of $i b.txt")
        end
    end
end

# ╔═╡ 7bce435c-f26e-11ea-1e8f-c51d17b66d67
begin
	#------------
	# Git tree
	
	# Initialize git repo.
	# TODO: Need a way to wrap the init lifecycle up in DataSets?
	rm("scratch/foo_git", recursive=true, force=true)
	mkdir("scratch/foo_git")
	run(setenv(`git init`, dir="scratch/foo_git"))

	# Version 1
	open(DataSets.GitTreeRoot("scratch/foo_git", write=true)) do gtree
	    copy!(gtree, tz)
	end
	
	sleep(1)

	# Version 2
	open(DataSets.GitTreeRoot("scratch/foo_git", write=true)) do gtree
	    append_tree(gtree)
	end
	
	# Version 3
	open(DataSets.GitTreeRoot("scratch/foo_git", write=true)) do gtree
	    append_tree(gtree)
	end
	
	# Show git logs
	Text(String(read(setenv(`git log`, dir="scratch/foo_git"))))
end

# ╔═╡ 867200ec-f272-11ea-0988-45c7ea485ce7
md"""
# Data Projects

* A collection of datasets relevant to a project.
* Maintaince via a data REPL.
"""

# ╔═╡ c494d9c6-f242-11ea-34e8-f99de0f01005
md"""
# Data REPL

What's it for?
* Linking and unlinking to existing datasets
* Copying/caching data
* Listing available datasets in data project
"""

# ╔═╡ b80197c8-f240-11ea-0804-09c0a5ff2fd2
md"""
# Tree Interface

## Paths and Roots

What is a **path root**? It's a location for a data resource, including enough information to open that resource.

What is a **relative path**, in general? It's a *key* into a heirarchical tree-structured data store. This consists of several path *components* (an array of strings)
"""

# ╔═╡ 5357dc56-f243-11ea-05b3-8fcd578c4c11
root = FileTreeRoot("scratch/foo")

# ╔═╡ bb296012-f242-11ea-2988-8f02471cbb85
relpath = path"d01/a.txt"

# ╔═╡ 93bc4b08-f2dc-11ea-3356-a72de92780c1
tree = DataSets.FileTree(root)

# ╔═╡ 3beae2da-f2dd-11ea-2bed-7346c6b8dfb7
md"""
**Trees may be indexed with path components or paths**
"""

# ╔═╡ 4da28a32-f2dd-11ea-10f5-5546fd84f217
tree["d01"]

# ╔═╡ 32f14aa2-f2dd-11ea-327d-9be3c39cd9d0
tree[relpath]

# ╔═╡ fe8d9110-f2e5-11ea-25ba-29bc56461235
tree["d01"]["a.txt"]

# ╔═╡ 7f380b36-f2dc-11ea-2b81-9559b5d7cea8
md"""
## Iteration

Iteration over a tree yields a list of children. Children may be:
* Another tree; `isdir(child) == true`
* Leaf data
"""

# ╔═╡ b320618c-f2dc-11ea-1216-a3f447f69d73
collect(tree)

# ╔═╡ 835fa5ce-f24c-11ea-32c0-019c58319ed3
md"""
# Etc
"""

# ╔═╡ 878842fa-f26a-11ea-02b9-d14b6483e9df
md"""
# Properties of DataSets

We need to deal with several abstractions for effective distributed
processing

* keys - the user works in terms of keys, eg, the indices of an array, the
  elements of a set, etc.
* indices - allow data to be looked up via the keys, quickly.
* partitions - in distributed settings large datasets are partitioned across
  machines.
"""

# ╔═╡ 81ce8f7c-f24c-11ea-3f39-e109699d6fb5
html"<button onclick=present()>Presentation Mode</button>"

# ╔═╡ dee541fa-f26b-11ea-0ee7-35b72ccec948
vega_data = DataSet(default_name="Foo",
			location=URI("file:///home/chris/.julia/packages/VegaDatasets/O079x/data/data"),
			decoders=["tree"])

# ╔═╡ fa5e979a-f26d-11ea-1e0f-15a4f243cdce
showtree(open(vega_data))

# ╔═╡ 0778cdec-f401-11ea-0b13-bfad3f49fee1
md"""
# Configurable open()
"""

# ╔═╡ Cell order:
# ╟─794887e2-f240-11ea-3f51-2d44e98e181c
# ╟─48da7d86-f240-11ea-1627-a33e327d3043
# ╟─5a7f286a-f269-11ea-12a6-f9ff81348191
# ╟─62cfc2c6-f26f-11ea-031d-f3a16062e3e8
# ╠═ab03a5aa-f26d-11ea-29fa-d5b93e2abd54
# ╠═1dbc4580-f26d-11ea-09e9-6b26b583cca2
# ╠═cb5510fa-f26d-11ea-108d-f11660a2288c
# ╟─ac4da728-f270-11ea-0c79-cb2691e3845c
# ╠═588ee8d4-f270-11ea-22f0-514b725b3c1e
# ╠═d07dca2c-f270-11ea-25b8-0756b5e518db
# ╠═6c2147f4-f270-11ea-2378-e74edb8bae91
# ╟─76dfef78-f249-11ea-00ca-f32459993b38
# ╟─0a9e0862-f402-11ea-2b96-533289dcb156
# ╟─565883ee-f26e-11ea-31ec-eb89f6019af7
# ╠═7bce435c-f26e-11ea-1e8f-c51d17b66d67
# ╟─867200ec-f272-11ea-0988-45c7ea485ce7
# ╟─c494d9c6-f242-11ea-34e8-f99de0f01005
# ╟─b80197c8-f240-11ea-0804-09c0a5ff2fd2
# ╠═5357dc56-f243-11ea-05b3-8fcd578c4c11
# ╠═bb296012-f242-11ea-2988-8f02471cbb85
# ╠═93bc4b08-f2dc-11ea-3356-a72de92780c1
# ╟─3beae2da-f2dd-11ea-2bed-7346c6b8dfb7
# ╠═4da28a32-f2dd-11ea-10f5-5546fd84f217
# ╠═32f14aa2-f2dd-11ea-327d-9be3c39cd9d0
# ╠═fe8d9110-f2e5-11ea-25ba-29bc56461235
# ╠═7f380b36-f2dc-11ea-2b81-9559b5d7cea8
# ╠═b320618c-f2dc-11ea-1216-a3f447f69d73
# ╟─835fa5ce-f24c-11ea-32c0-019c58319ed3
# ╟─878842fa-f26a-11ea-02b9-d14b6483e9df
# ╟─81ce8f7c-f24c-11ea-3f39-e109699d6fb5
# ╠═dee541fa-f26b-11ea-0ee7-35b72ccec948
# ╠═fa5e979a-f26d-11ea-1e0f-15a4f243cdce
# ╟─0778cdec-f401-11ea-0b13-bfad3f49fee1
