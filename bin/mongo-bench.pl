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

use MongoBench qw/get_dataset run_test MOST_DOCS SOME_DOCS/;

use DateTime;
use MongoDB;

my $host = exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost';
my $client = MongoDB::MongoClient->new(host => $host, w => 1);
my $db = $client->get_database("benchdb");
$db->drop;

# Print test info
my $build = $client->get_database( 'admin' )->get_collection( '$cmd' )->find_one( { buildInfo => 1 } );
my ($version_str) = $build->{version} =~ m{^([0-9.]+)};
my $server_version = version->parse("v$version_str");
print "Driver $MongoDB::VERSION, Perl v$], MongoDB $server_version\n";

my ($schema, %dataset) = get_dataset();
if (%dataset) {

    my $read_coll = $db->get_collection("read");
    my $insert_coll = $db->get_collection("inserts");

    # Fill db for read with dataset
    {
        print "Importing dataset...";
        STDOUT->flush();

        $read_coll->drop;
        my $bulk = $read_coll->initialize_ordered_bulk_op;
        $bulk->insert($_) for @{$dataset{all_docs}};
        $bulk->execute;
        print " done\n";
    }

    # Creating
    # These benchmarks should be bottlenecked by BSON encoding
    # based on the dataset provided.
    {

        my @small_doc_some = ($dataset{smallest_doc}) x SOME_DOCS;
        my @small_doc_most = ($dataset{smallest_doc}) x MOST_DOCS;
        my @large_doc_some = ($dataset{largest_doc}) x SOME_DOCS;
        my @large_doc_most = ($dataset{largest_doc}) x MOST_DOCS;

        my $data = [
            ["single insert" => $dataset{single_doc}],
            ["batch insert ".SOME_DOCS." documents" => $dataset{some_docs}],
            ["batch insert ".MOST_DOCS." documents" => $dataset{most_docs}],
            ["single insert smallest document" => $dataset{smallest_doc}],
            ["batch insert ".SOME_DOCS." small documents" => \@small_doc_some],
            ["batch insert ".MOST_DOCS." small documents"=> \@small_doc_most],
            ["single insert largest document" => $dataset{largest_doc}],
            ["batch insert ".SOME_DOCS." large documents" => \@large_doc_some],
            ["batch insert ".MOST_DOCS." large documents" => \@large_doc_most],
        ];

        for my $benchmark (@$data) {

            run_test($benchmark->[0], sub { $insert_coll->insert($benchmark->[1], {safe => 1}) } );
        }

        # Bulk creation
        {
            my $bulk_data = [
                ["all documents" => $dataset{all_docs}],
                [SOME_DOCS." small documents" => \@small_doc_some],
                [MOST_DOCS." small documents"=> \@small_doc_most],
                [SOME_DOCS." large documents" => \@large_doc_some],
                [MOST_DOCS." large documents" => \@large_doc_most],
            ];

            for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {

                for my $benchmark (@$bulk_data) {

                    run_test("$method on $benchmark->[0]", sub {

                        my $bulk = $insert_coll->$method;
                        $bulk->insert($_, {w => 1}) for @{$benchmark->[1]};
                        $bulk->execute;
                    });
                }
            }
        }
    }

    # Reading
    # These benchmarks should be bottlenecked by BSON decoding
    # based on the dataset provided.
    {
        run_test("find_one simple", sub { $read_coll->find_one } );

        run_test("find_one query", sub { $read_coll->find_one($schema->{find_spec}) } );

        run_test("find all and iterate", sub {

            my $cursor = $read_coll->find();
            () while $cursor->next;
        });

        run_test("find on query and iterate", sub {

            my $cursor = $read_coll->find($schema->{find_spec});
            () while $cursor->next;
        });
    }

    # Updating
    {
        # We're about to clobber read so these tests should be performed last.
        run_test("update one w/ inc" => sub { $read_coll->update({}, {'$inc' => {"newField" => 123}}) } );

        run_test("update all w/ inc" => sub { $read_coll->update({}, {'$inc' => {"newField" => 123}}) }, {'multiple' => 1} );

        run_test("update one w/ small documents" => sub { $read_coll->update({}, $dataset{smallest_doc}) } );

        run_test("update one w/ large documents", sub { $read_coll->update({}, $dataset{largest_doc}) } );
    }

} else {

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

                run_test("insert $doc_size ".$benchmark->[0], sub { $encode_coll->insert($benchmark->[1], {safe => 1}) } );
            }
        }

        # BSON Decoding
        {
            for my $benchmark (@$data) {

                run_test("find $doc_size ".$benchmark->[0], sub {
                        local $MongoDB::BSON::use_boolean = 1;
                        local $MongoDB::BSON::use_binary = 1;
                        $decode_coll->find_one({type => $benchmark->[0]});
                        $MongoDB::BSON::use_boolean = 0;
                        $MongoDB::BSON::use_binary = 0;
                });
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

        run_test("insert 4kb file", sub {

            open( my $tempfh, '<', \$str_4kib);
            $grid->insert($tempfh, {safe => 1});
        });
        run_test("insert 500kb file", sub {

            open( my $tempfh, '<', \$str_500kib);
            $grid->insert($tempfh, {safe => 1});
        });
        run_test("find 4kb file", sub {

            $grid->find_one({filename => "4KiB"});
        });
        run_test("find 500kb file", sub {

            $grid->find_one({filename => "500KiB"});
        });
    }
}

END {

    # Cleanup
    $db->drop if $db;
}
