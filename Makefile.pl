use strict;
use warnings;
use inc::Module::Install;

name 'MongoDB-Performance';
perl_version '5.8.4';
author 'Eric Daniels <eric.daniels@mongodb.com';
license 'Apache';
version_from 'bin/mongo-bench.pl';
install_script 'bin/mongo-bench.pl';

requires 'Benchmark';
requires 'Devel::NYTProf';
requires 'Getopt::Long';
requires 'IO::Handle';
requires 'JSON::XS';
requires 'Path::Class';
requires 'Pod::Usage';
requires 'Text::CSV_XS';
requires 'version';

repository 'git@github.com:edaniels/mongo-perl-performance.git';

WriteAll;
