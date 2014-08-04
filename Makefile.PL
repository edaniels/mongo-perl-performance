use strict;
use warnings;
use inc::Module::Install;

name 'MongoDB-Performance';
perl_version '5.8.4';
author 'Eric Daniels <eric.daniels@mongodb.com';
license 'Apache';
install_script ('bin/mongo-bench.pl', 'bin/bson-bench.pl');
all_from 'lib/MongoBench.pm';

requires 'Benchmark';
requires 'BSON';
requires 'Devel::NYTProf';
requires 'Getopt::Long';
requires 'IO::Handle';
requires 'JSON::XS';
requires 'Path::Tiny';
requires 'Pod::Usage';
requires 'version';

repository 'git@github.com:edaniels/mongo-perl-performance.git';

WriteAll;

