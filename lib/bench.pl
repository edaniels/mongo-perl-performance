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
    my $file_path = $_[0];
    my $line_limit = $_[1] == -1 ? 0+'inf' : $_[1];

    open(my $fh, "<:encoding(UTF-8)", $file_path) or die("Can't open $file_path: $!\n");
    my @docs;
    for (my $line_num = 0, my $line = <$fh>; $line_num < $line_limit && $line; $line = <$fh>, $line_num++) {

        push @docs, decode_json($line);
    }
    return @docs;
}

sub create_dataset {

    my $data = $_[0];

    my %dataset = (

        all => $data,
        single_doc => $data->[0],
        docs_100 => [@$data[1..100]],
        docs_1000 => [@$data[1..1000]],
    );

    return %dataset;
}

my @tweets = get_data_from_json($dataset_path, 2_000);
my %dataset = create_dataset(\@tweets);
my %scheme = (

    find_spec => {source=>"web"}
);

my $client = MongoDB::MongoClient->new;
my $db = $client->get_database("benchdb");

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

    timethese( -2, {

        "single insert" => sub {

            profile( sub { $insert_coll->insert($dataset{single_doc}) } );
        },

        "batch insert 100" => sub {

            profile( sub { $insert_coll->insert($dataset{docs_100}) } );
        },

        "batch insert 1000" => sub {

            profile( sub { $insert_coll->insert($dataset{docs_1000}) } );
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

# Deleting

END {

    DB::finish_profile if $profile;
    $db->get_collection("read")->drop if $db;
}