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

use BSON qw/encode decode/;
use DateTime;

print "BSON $BSON::VERSION, Perl v$]\n";

my ($schema, %dataset) = get_dataset();
if (%dataset) {

    my $data;
    {
        my $small_doc_some = {arr => [($dataset{smallest_doc}) x SOME_DOCS]};
        my $small_doc_most = {arr => [($dataset{smallest_doc}) x MOST_DOCS]};
        my $large_doc_some = {arr => [($dataset{largest_doc}) x SOME_DOCS]};
        my $large_doc_most = {arr => [($dataset{largest_doc}) x MOST_DOCS]};

        $data = [
            ["single document" => $dataset{single_doc}],
            ["smallest document" => $dataset{smallest_doc}],
            ["largest document" => $dataset{largest_doc}],
            [SOME_DOCS." documents as nested array" => $dataset{some_docs}],
            [MOST_DOCS." documents as nested array" => $dataset{most_docs}],
            [SOME_DOCS." small documents as nested array" => $small_doc_some],
            [MOST_DOCS." small documents as nested array" => $small_doc_most],
            [SOME_DOCS." large documents as nested array" => $large_doc_some],
            [MOST_DOCS." large documents as nested array" => $large_doc_most],
            ["all documents as nested array" => $dataset{all_docs}],
        ];
    }

    # Encoding
    {
        for my $benchmark (@$data) {

            run_test("encode ".$benchmark->[0], sub { encode($benchmark->[1]) } );
        }
    }

    # Decoding
    {
        for my $benchmark (@$data) {

            my $encoded_document = encode($benchmark->[1]);
            run_test("decode ".$benchmark->[0], sub { decode($encoded_document) } );
        }
    }

} else {

    my $doc_size = 100;

    # Create dummy data
    my $string_100 = ('doyouevenbenchbroham') x 5;
    my $string_10000 = ('doyouevenbenchbroham') x 500;
    my $utf8_100 = ("\x64\x6F\x79\x6F\x75\x65\x76\x65\x6E\x62\x65\x6E\x63\x68\x62\x72\x6F\x68\x61\x6D") x 5;
    my $oid = BSON::ObjectId->new("49b6d9fb17330414a0c63102");
    my $date = DateTime->from_epoch(epoch => 1271079861);
    my $array = ["if", 3, "nodes", "in", "a", "replica", "set", "fail"];
    my $binary = {
        bindata => [
           BSON::Binary->new($string_100),
           BSON::Binary->new($string_100, 0x00),
           BSON::Binary->new($string_100, 0x01),
           BSON::Binary->new($string_100, 0x03),
           BSON::Binary->new($string_100, 0x05),
           BSON::Binary->new($string_100, 0x80)
        ]
    };

    my %types = (
        string_100 => $string_100,
        string_10000 => $string_10000,
        utf8_100 => $utf8_100,
        oid => BSON::ObjectId->new("49b6d9fb17330414a0c63102"),
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
    my $data = [];
    for my $type (qw/undef int double string_100 string_10000 utf8_100 boolean oid date array nested binary/) {

        my %doc;
        if (ref $types{$type} eq 'CODE') {
            $doc{$_} = &{$types{$type}} for 1..$doc_size;
        } else {
            $doc{$_} = $types{$type} for 1..$doc_size;
        }

        push(@$data, [$type, \%doc]);
    }

    # Encode
    {
        for my $benchmark (@$data) {

            run_test("encode $doc_size ".$benchmark->[0], sub { encode($benchmark->[1]) });
        }
    }

    # Decode
    {
        for my $benchmark (@$data) {

            my $encoded_document = encode($benchmark->[1]);
            run_test("decode $doc_size ".$benchmark->[0], sub { decode($encoded_document) });
        }
    }
}
