use strict;
use warnings;
use inc::Module::Install;

name 'MongoDB-Performance';
perl_version '5.8.4';
author 'Eric Daniels <eric.daniels@mongodb.com';
license 'Apache';
version_from 'bin/mongo-bench.pl';
install_script 'bin/mongo-bench.pl';

requires 'IO::Handle';
requires 'Getopt::Long';
requires 'Benchmark';
requires 'Devel::NYTProf';
requires 'JSON::XS';
requires 'Pod::Usage';
requires 'MongoDB';
requires 'version';

repository 'git@github.com:edaniels/mongo-perl-performance.git';

WriteAll;
