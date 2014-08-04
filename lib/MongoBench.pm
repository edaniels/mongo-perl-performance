#!/usr/bin/env perl

#
#  Copyright 2009-2014 MongoDB, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

use 5.008;
use strict;
use warnings;

use constant {

    MOST_DOCS => 1000,
    SOME_DOCS => 100,
    CPU_RUN_TIME => 2,
};

my ($profile, $bench, $dataset_path, $lines_to_read, $profile_out, $bench_out);

BEGIN {

=head1 SYNOPSIS

bench.pl [options]

Options:
    -profile            enable profiling
    -profileout         file to output profiling data to (default : mongo-perl-prof.out)
    -bench              enable benchmarking
    -benchout           file to output benchmark data to (default: report.json)
    -dataset            dataset to use. expects a schema to exist in the form of dataset.schema.json
    -lines              lines to read from dataset (default: 1000)

=cut

    $profile = '';
    $bench = '';
    $dataset_path = '';
    $lines_to_read = MOST_DOCS;
    $profile_out = 'mongo-perl-prof.out';
    $bench_out = 'report.json';

    use Getopt::Long;
    use Pod::Usage;
    GetOptions(
        "profile" => \$profile,
        "profileout=s" => \$profile_out,
        "bench" => \$bench,
        "benchout=s" => \$bench_out,
        "dataset=s" => \$dataset_path,
        "lines=i" => \$lines_to_read,
    ) or pod2usage(2);

    unshift(@INC, ('blib/lib', 'blib/arch'));

    if ($profile) {

        $ENV{NYTPROF} = "file=$profile_out:";
        require Devel::NYTProf;
        Devel::NYTProf->import();
    }

    print "Running against known dataset since none was provided\n" if !$dataset_path;
}

use Benchmark qw/:all/;
use IO::Handle qw//;
use JSON::XS;
use Path::Tiny;
use version;

our @EXPORT_OK = qw/get_dataset run_test MOST_DOCS SOME_DOCS/;
our $VERSION = 'v0.0.1';

my %bench_results; # treated as a global

sub get_dataset {

    my ($schema, %dataset);

    if ($dataset_path) {

        my ($dataset_size, $schema, @data) = get_data_from_json($dataset_path, $lines_to_read);
        %dataset = create_dataset(\@data);
        print "Dataset: " . path($dataset_path)->basename . ", size: $dataset_size\n";
    }

    return $schema, %dataset;
}

# run_test will run either benchmarking, profiling, or both on a given function
sub run_test {

    my ($test_name, $func) = @_;

    $bench_results{$test_name} = timethis(-(CPU_RUN_TIME), $func, $test_name) if $bench;
    &$func if $profile;
}

sub get_data_from_json {

    die "lines must be greater than 0 or -1 (no limit)" unless $_[1] > 0 || $_[1] == -1;
    my $file_path = $_[0];
    my $line_limit = $_[1] == -1 ? 0+'inf' : $_[1];
    my $schema_path = $file_path;
    $schema_path =~ s/(\.json)$/.schema$1/;

    open(my $dataset_fh, "<:encoding(UTF-8)", $file_path) or die("Can't open $file_path: $!\n");
    open(my $schema_fh, "<:encoding(UTF-8)", $schema_path) or die("Can't open $schema_path: $!\n");

    # Read schema
    my $schema;
    {
        local $/; # no delimiter so we read everything till EOF
        $schema = decode_json(<$schema_fh>);
    }

    my (@docs, $smallest_doc, $largest_doc);

    # Collect all documents
    my $line_num;
    for ($line_num = 0, my $line = <$dataset_fh>; $line_num < $line_limit && $line; $line = <$dataset_fh>, $line_num++) {

        $smallest_doc ||= $line;
        $largest_doc ||= $line;

        # Keep tracking the largest and smallest documents but never decode
        $largest_doc = $line if length $line > length $largest_doc;
        $smallest_doc = $line if length $line < length $smallest_doc;
        push @docs, decode_json($line);
    }

    die "Need at least " . MOST_DOCS . " documents; got $line_num" unless $line_num >= MOST_DOCS;

    push @docs, decode_json($smallest_doc);
    push @docs, decode_json($largest_doc);
    return $line_num, $schema, @docs;
}

sub create_dataset {

    my $data = $_[0];

    my %dataset = (
        all_docs => {arr => $data},
        single_doc => $data->[0],
        some_docs => {arr => [@$data[0..(SOME_DOCS-1)]]},
        most_docs => {arr => [@$data[0..(MOST_DOCS-1)]]},
        largest_doc => $data->[-1],
        smallest_doc => $data->[-2],
    );

    return %dataset;
}

sub write_bench_results {

    my %results = %{+shift};
    my @docs;
    while (my ($key, $value) = each %results) {

        push(@docs, {

            title => $key,
            rounds => 1,
            iterations => $value->iters,
            timeReal => $value->real,
            timeUser => $value->cpu_a,
            opsReal => $value->iters/$value->real,
            opsUser => $value->iters/$value->cpu_a,
        });
    }
    {
        open(my $fh, ">:encoding(utf8)", $bench_out) or die("Can't open $bench_out: $!\n");
        print $fh encode_json(\@docs);
    }
}

END {

    write_bench_results(\%bench_results) if $bench;
}

1;
