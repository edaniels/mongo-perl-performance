# Installation

	perl Makefile.PL
	make install

# Usage

	mongo-bench.pl [-dataset file.json] [-profile]
	
* **dataset** - Represents the dataset to use for the benchmark. Needs to be in JSON format delimited by new lines
	* Leaving out dataset will cause benchmarks to run on known data (e.g. BSON encoding/decoding, GridFS)
* **profile** - Setting profile will enable the profiler
	* **NOTE:** expect slower benchmarks while profiling is enabled

# Profiler

* In addition to setting the profile flag, you must set the NYTPROF variable in your shell:
	

		export NYTPROF=start=no:file=./mongo-perl-prof.out
	
* To view the data run:

		nytprofhtml --open --file mongo-perl-prof.out
		
 

	

