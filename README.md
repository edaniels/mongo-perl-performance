# Installation

	perl Makefile.PL
	make install

# Usage

	mongo-bench.pl [-dataset file.json] [-profile]
	
* **dataset** - Represents the dataset to use for the benchmark. Needs to be in JSON format delimited by new lines
	* A file.schema.json must be present where it is a json document containing certain documents.
	* Leaving out dataset will cause benchmarks to run on known data (e.g. BSON encoding/decoding, GridFS)
* **profile** - Setting profile will enable the profiler
	* **NOTE:** expect slower benchmarks while profiling is enabled

# Schema.json

* The schema file is a single json document containing the following fields: find_spec

## find_spec

* Must be a representative spec used in a find query.

# Profiler

* In addition to setting the profile flag, you must set the NYTPROF variable in your shell:
	

		export NYTPROF=start=no:file=./mongo-perl-prof.out
	
* To view the data run:

		nytprofhtml --open --file mongo-perl-prof.out
		
 

	

