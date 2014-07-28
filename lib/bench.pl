use 5.008;
use strict;
use warnings;
use IO::Handle qw//;
use Getopt::Long;
use Benchmark qw/:all/;
use Devel::NYTProf;
use JSON::XS;
use Pod::Usage;

use MongoDB;

my $cwd;
{
    use Cwd qw/abs_path/;
    use Path::Class qw/file/;
    $cwd = file(abs_path($0))->dir;
}

my $profile = '';
my $dataset_path = '';

=head1 SYNOPSIS

bench.pl [options]

Options:
    -profile            enable profiling
    -dataset            dataset to use

=cut
GetOptions(
    "profile" => \$profile,
    "dataset=s" => \$dataset_path,
) or pod2usage(2);

pod2usage(-verbose => 0, -message => "$0: dataset argument required\n") unless $dataset_path;

sub get_data_from_json {

    die "usage: get_twitter_data <file_path> <line_limit>" unless @_ == 2;
    die "<line_limit> must be greater than 0 or -1 (no limit)" unless $_[1] > 0 || $_[1] == -1;
    my $file_path = $_[0];
    my $line_limit = $_[1] == -1 ? 0+'inf' : $_[1];

    open(my $fh, "<:encoding(UTF-8)", $file_path) or die("Can't open $file_path: $!\n");

    my (@docs, $smallest_doc, $largest_doc);

    # Collect all documents
    my $line_num;
    for ($line_num = 0, my $line = <$fh>; $line_num < $line_limit && $line; $line = <$fh>, $line_num++) {

        $smallest_doc ||= $line;
        $largest_doc ||= $line;

        # Keep tracking the largest and smallest documents but never decode
        $largest_doc = $line if length $line > length $largest_doc;
        $smallest_doc = $line if length $line < length $smallest_doc;
        push @docs, decode_json($line);
    }

    push @docs, decode_json($smallest_doc);
    push @docs, decode_json($largest_doc);
    return @docs;
}

sub create_dataset {

    my $data = $_[0];

    my %dataset = (

        all => $data,
        single_doc => $data->[0],
        docs_100 => [@$data[1..100]],
        docs_1000 => [@$data[1..1_000]],
        largest_doc => $data->[-1],
        smallest_doc => $data->[-2],
    );

    return %dataset;
}

my @data = get_data_from_json($dataset_path, 2_000);
my %dataset = create_dataset(\@data);
my %scheme = (

    find_spec => {source=>"web"}
);

my $client = MongoDB::MongoClient->new;
my $db = $client->get_database("benchdb");

# Print test info
my $build = $client->get_database( 'admin' )->get_collection( '$cmd' )->find_one( { buildInfo => 1 } );
my ($version_str) = $build->{version} =~ m{^([0-9.]+)};
print "Driver $MongoDB::VERSION, Perl v$], MongoDB " . version->parse("v$version_str") . "\n";
print "Dataset: " . file($dataset_path)->basename . "\n";

sub profile {

    return &{$_[0]} unless $profile;
    DB::enable_profile;
    &{$_[0]};
    DB::disable_profile;
}

# Fill db for read with dataset
{
    print "Importing dataset...";
    STDOUT->flush();

    my $read_coll = $db->get_collection("read");
    $read_coll->drop;
    $read_coll->insert($_) for @{$dataset{all}};
    print " done.\n";
}

# Creating
{
    my $insert_coll = $db->get_collection("inserts");

    my @small_doc_100 = ($dataset{smallest_doc}) x 100;
    my @small_doc_1000 = ($dataset{smallest_doc}) x 1_000;
    my @large_doc_100 = ($dataset{largest_doc}) x 100;
    my @large_doc_1000 = ($dataset{largest_doc}) x 1_000;

    timethese( -2, {

        "single insert" => sub {

            profile( sub { $insert_coll->insert($dataset{single_doc}) } );
        },

        "batch insert 100" => sub {

            profile( sub { $insert_coll->insert($dataset{docs_100}) } );
        },

        "batch insert 1000" => sub {

            profile( sub { $insert_coll->insert($dataset{docs_1000}) } );
        },

        "single insert small doc" => sub {

            profile( sub { $insert_coll->insert($dataset{smallest_doc}) } );
        },

        "batch insert 100 small doc" => sub {

            profile( sub { $insert_coll->insert(\@small_doc_100) } );
        },

        "batch insert 1000 small doc" => sub {

            profile( sub { $insert_coll->insert(\@small_doc_1000) } );
        },

        "single insert large doc" => sub {

            profile( sub { $insert_coll->insert($dataset{largest_doc}) } );
        },

        "batch insert 100 large doc" => sub {

            profile( sub { $insert_coll->insert(\@large_doc_100) } );
        },

        "batch insert 1000 large doc" => sub {

            profile( sub { $insert_coll->insert(\@large_doc_1000) } );
        }
    });

    $insert_coll->drop;
}

# Reading
{
    my $read_coll = $db->get_collection("read");

    timethese( -2, {

        "find_one simple" => sub {
            profile( sub { $read_coll->find_one } );
        },

        "find_one query" => sub {
            profile( sub { $read_coll->find_one($scheme{find_spec}) } );
        },

        "find all and iterate" => sub { profile( sub {

            my $cursor = $read_coll->find();
            () while $cursor->next;
        })},

        "find on query and iterate" => sub { profile( sub {

            my $cursor = $read_coll->find($scheme{find_spec});
            () while $cursor->next;
        })},
    });
}

# Updating
{
    my $read_coll = $db->get_collection("read");

    timethese( -2, {

        "update one w/ set" => sub {
            profile( sub { $read_coll->update({}, {'$set' => {"newField" => 123}}) } );
        },

        "update all w/ set" => sub {
            profile( sub { $read_coll->update({}, {'$set' => {"newField" => 123}}) }, {'multiple' => 1} );
        },

        "update one w/ small doc" => sub {
            profile( sub { $read_coll->update({}, $dataset{smallest_doc}) } );
        },

        "update one w/ large doc" => sub {
            profile( sub { $read_coll->update({}, $dataset{largest_doc}) } );
        },
    });
}

# Deleting

END {

    DB::finish_profile if $profile;
    $db->get_collection("read")->drop if $db;
}