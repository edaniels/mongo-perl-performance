# Requirements

	* Module::Install is required to build and install the project
	* mongod instance running at localhost:27017 or MONGOD environment variable set with appropriate URI

# Installation

	perl Makefile.PL
	make install

# Usage

	mongo-bench.pl [-dataset [-line]] < [-profile [-profileout]] | [-bench [-benchout]|[-benchuri [-benchdb] [-benchcoll]]] >
	
* **dataset** - Represents the dataset to use for the benchmark. Needs to be in JSON format delimited by new lines
	* A file.schema.json must be present where it is a json document containing certain documents.
	* Leaving out dataset will cause benchmarks to run on known data (e.g. BSON encoding/decoding, GridFS)
* **lines** - Lines to read from dataset (default: 1000)
* **profile** - Setting bench will enable profiling (default: false)
* **profileout** - File to save profiling output to (default: mongo-perl-perf.out)
* **bench** - Setting bench will enable benchmarking (default: false)
* **benchout** - File to save benchmaking json output to (default: report.json)
* **benchuri** - MongoDB Connection String pointing to host to store benchmark results with
* **benchdb** - DB to store benchmark results in (default: bench_results)
* **benchcoll** - Collection to store benchmark results in (default: perl)

# Schema.json

* The schema file is a single json document containing the following fields: find_spec

## find_spec

* Must be a representative spec used in a find query.

# Profiler
	
* To view the profiling data run:

		nytprofhtml --open --file mongo-perl-prof.out

* Optionally you can view the data in a call graph viewer like qcachegrind by running:

		nytprofcg --file mongo-perl-prof.out --out mongo-perl-prof.callgrind
