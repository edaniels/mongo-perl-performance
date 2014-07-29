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
use Benchmark qw/:all/;
use Devel::NYTProf;
use Getopt::Long;
use IO::Handle qw//;
use JSON::XS;
use MongoDB;
use Pod::Usage;
use version;
our $VERSION = 'v0.0.1';

use constant {

    MOST_DOCS => 1000,
    SOME_DOCS => 100,
};

my $profile = '';
my $dataset_path = '';
my $lines_to_read = MOST_DOCS;

=head1 SYNOPSIS

bench.pl [options]

Options:
    -profile            enable profiling
    -dataset            dataset to use. expects a schema to exist in the form of dataset.schema.json
    -lines              lines to read from dataset (default: 1000)

=cut
GetOptions(
    "profile" => \$profile,
    "dataset=s" => \$dataset_path,
    "lines=i" => \$lines_to_read,
) or pod2usage(2);

my $client = MongoDB::MongoClient->new;
my $db = $client->get_database("benchdb");
$db->drop;

# Print test info
my $build = $client->get_database( 'admin' )->get_collection( '$cmd' )->find_one( { buildInfo => 1 } );
my ($version_str) = $build->{version} =~ m{^([0-9.]+)};
my $server_version = version->parse("v$version_str");
print "Driver $MongoDB::VERSION, Perl v$], MongoDB $server_version\n";

if ($dataset_path) {

    my ($dataset_size, $schema, @data) = get_data_from_json($dataset_path, $lines_to_read);
    my %dataset = create_dataset(\@data);
    print "Dataset: " . file($dataset_path)->basename . ", size: $dataset_size\n";

    # Fill db for read with dataset
    {
        print "Importing dataset...";
        STDOUT->flush();

        my $read_coll = $db->get_collection("read");
        $read_coll->drop;
        $read_coll->insert($_) for @{$dataset{all_docs}};
        print " done\n";
    }

    # Creating
    # These benchmarks should be bottlenecked by BSON encoding
    # based on the dataset provided.
    {
        my $insert_coll = $db->get_collection("inserts");

        my @small_doc_some = ($dataset{smallest_doc}) x SOME_DOCS;
        my @small_doc_most = ($dataset{smallest_doc}) x MOST_DOCS;
        my @large_doc_some = ($dataset{largest_doc}) x SOME_DOCS;
        my @large_doc_most = ($dataset{largest_doc}) x MOST_DOCS;

        my $data = [
            ["single insert" => $dataset{single_doc}],
            ["batch insert ".SOME_DOCS." docs" => $dataset{some_docs}],
            ["batch insert ".MOST_DOCS." docs" => $dataset{most_docs}],
            ["single insert small doc" => $dataset{smallest_doc}],
            ["batch insert ".SOME_DOCS." small doc" => \@small_doc_some],
            ["batch insert ".MOST_DOCS." small doc"=> \@small_doc_most],
            ["single insert large doc" => $dataset{largest_doc}],
            ["batch insert ".SOME_DOCS." large doc" => \@large_doc_some],
            ["batch insert ".MOST_DOCS." large doc" => \@large_doc_most],
        ];

        for my $benchmark (@$data) {

            my $insert_func = sub {

                profile(sub { $insert_coll->insert($benchmark->[1]) });
            };

            timethis(-2, $insert_func, $benchmark->[0]);
        }
    }

    # Reading
    # These benchmarks should be bottlenecked by BSON decoding
    # based on the dataset provided.
    {
        my $read_coll = $db->get_collection("read");

        timethese( -2, {

            "find_one simple" => sub {
                profile( sub { $read_coll->find_one } );
            },

            "find_one query" => sub {
                profile( sub { $read_coll->find_one($schema->{find_spec}) } );
            },

            "find all and iterate" => sub { profile( sub {

                my $cursor = $read_coll->find();
                () while $cursor->next;
            })},

            "find on query and iterate" => sub { profile( sub {

                my $cursor = $read_coll->find($schema->{find_spec});
                () while $cursor->next;
            })},
        });
    }

    # Updating
    {
        # We're about to clobber read so these tests should be performed last.
        my $read_coll = $db->get_collection("read");

        timethese( -2, {

            "update one w/ inc" => sub {
                profile( sub { $read_coll->update({}, {'$inc' => {"newField" => 123}}) } );
            },

            "update all w/ inc" => sub {
                profile( sub { $read_coll->update({}, {'$inc' => {"newField" => 123}}) }, {'multiple' => 1} );
            },

            "update one w/ small doc" => sub {
                profile( sub { $read_coll->update({}, $dataset{smallest_doc}) } );
            },

            "update one w/ large doc" => sub {
                profile( sub { $read_coll->update({}, $dataset{largest_doc}) } );
            },
        });
    }

} else {

    print "Running against known dataset since none was provided\n";

    # BSON
    # These benchmarks indirectly test BSON through insert and find.
    {
        use MongoDB::BSON::Binary;

        my $doc_size = 100;

        # Create dummy data
        my $string_100 = ('doyouevenbenchbroham') x 5;
        my $string_10000 = ('doyouevenbenchbroham') x 500;
        my $utf8_100 = ("\x64\x6F\x79\x6F\x75\x65\x76\x65\x6E\x62\x65\x6E\x63\x68\x62\x72\x6F\x68\x61\x6D") x 5;
        my $oid = MongoDB::OID->new("49b6d9fb17330414a0c63102");
        my $date = DateTime->from_epoch(epoch => 1271079861);
        my $array = ["if", 3, "nodes", "in", "a", "replica", "set", "fail"];
        my $binary = {
            bindata => [
               MongoDB::BSON::Binary->new(data => $string_100),
               MongoDB::BSON::Binary->new(data => $string_100, subtype => MongoDB::BSON::Binary->SUBTYPE_GENERIC),
               MongoDB::BSON::Binary->new(data => $string_100, subtype => MongoDB::BSON::Binary->SUBTYPE_FUNCTION),
               MongoDB::BSON::Binary->new(data => $string_100, subtype => MongoDB::BSON::Binary->SUBTYPE_GENERIC_DEPRECATED),
               MongoDB::BSON::Binary->new(data => $string_100, subtype => MongoDB::BSON::Binary->SUBTYPE_UUID_DEPRECATED),
               MongoDB::BSON::Binary->new(data => $string_100, subtype => MongoDB::BSON::Binary->SUBTYPE_UUID),
               MongoDB::BSON::Binary->new(data => $string_100, subtype => MongoDB::BSON::Binary->SUBTYPE_MD5),
               MongoDB::BSON::Binary->new(data => $string_100, subtype => MongoDB::BSON::Binary->SUBTYPE_USER_DEFINED)
            ]
        };

        my %types = (
            string_100 => $string_100,
            string_10000 => $string_10000,
            utf8_100 => $utf8_100,
            oid => MongoDB::OID->new("49b6d9fb17330414a0c63102"),
            date => DateTime->from_epoch(epoch => 1271079861),
            array => ["if", 3, "nodes", "in", "a", "replica", "set", "fail"],
            nested => {
                perl => "doesnt",
                guarantee => "ordering",
                in => 4,
                hash => "structure",
                well => $array,
                that => $string_100,
                is => $date,
                unfortunate => $oid
            },
            binary => $binary,
            int => sub { return int(rand(2147483647)) },
            double => sub { return rand(2147483647) },
            boolean => sub { use boolean; return boolean(int(rand(2))) },
            undef => undef,
        );

        # Create docs of $doc_size elements
        my $decode_coll = $db->get_collection("bson_decode");
        my $data = [];
        for my $type (qw/undef int double string_100 string_10000 utf8_100 boolean oid date array nested binary/) {

            my %doc;
            if (ref $types{$type} eq 'CODE') {
                $doc{$_} = &{$types{$type}} for 1..$doc_size;
            } else {
                $doc{$_} = $types{$type} for 1..$doc_size;
            }

            # Add to bson_decode
            $decode_coll->insert({type => $type, value => \%doc});
            push(@$data, [$type, \%doc]);
        }

        # BSON Encoding
        {
            my $encode_coll = $db->get_collection("bson_encode");

            for my $benchmark (@$data) {

                my $insert_func = sub {

                    profile(sub { $encode_coll->insert($benchmark->[1]) });
                };

                timethis(-2, $insert_func, "insert $doc_size ".$benchmark->[0]);
            }
        }

        # BSON Decoding
        {
            for my $benchmark (@$data) {

                my $find_func = sub {

                    profile(sub {
                        local $MongoDB::BSON::use_boolean = 1;
                        local $MongoDB::BSON::use_binary = 1;
                        $decode_coll->find_one({type => $benchmark->[0]});
                        $MongoDB::BSON::use_boolean = 0;
                        $MongoDB::BSON::use_binary = 0;
                    });
                };

                timethis(-2, $find_func, "find $doc_size ".$benchmark->[0]);
            }
        }
    }

    # GridFS
    # This benchmark is independent of the dataset and as such uses a known dataset. We can get
    # away with doing this here since we are only dealing with opqaue bytes.
    {

        my $str_4kib = ('1') x (4 * 1024);
        my $str_500kib = ('1') x (500 * 1024);
        open( my $fh, '<', \$str_4kib);
        open( my $fh2, '<', \$str_500kib);

        # Prepare for reads
        my $grid = $db->get_gridfs;
        $grid->insert($fh, {"filename" => "4KiB"});
        $grid->insert($fh2, {"filename" => "500KiB"});

        timethese( -2, {

            "insert 4kb file" => sub {

                open( my $tempfh, '<', \$str_4kib);
                profile( sub { $grid->insert($tempfh) } );
            },

            "insert 500kb file" => sub {

                open( my $tempfh, '<', \$str_500kib);
                profile( sub { $grid->insert($tempfh) } );
            },

            "find 4kb file" => sub {

                open( my $tempfh, '<', \$str_4kib);
                profile( sub { $grid->find_one({filename => "4KiB"}) } );
            },

            "find 500kb file" => sub {

                open( my $tempfh, '<', \$str_500kib);
                profile( sub { $grid->find_one({filename => "500KiB"}) } );
            },
        });
    }
}

# profile will only enable profiling if the user has requested it. Otherwise we
# try to execute the function in question with as little overhead as possible in
# order to get accurate benchmark times.
sub profile {

    return &{$_[0]} unless $profile;
    DB::enable_profile;
    &{$_[0]};
    DB::disable_profile;
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
        local $/;
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
        all_docs => $data,
        single_doc => $data->[0],
        some_docs => [@$data[0..(SOME_DOCS-1)]],
        most_docs => [@$data[0..(MOST_DOCS-1)]],
        largest_doc => $data->[-1],
        smallest_doc => $data->[-2],
    );

    return %dataset;
}

END {

    # Cleanup
    DB::finish_profile if $profile;
    $db->drop if $db;
}