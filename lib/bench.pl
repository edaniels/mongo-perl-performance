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

use constant {

    MOST_DOCS => 1000,
    SOME_DOCS => 100,
};

sub get_data_from_json {

    die "usage: get_data_from_json <file_path> <line_limit>" unless @_ == 2;
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

    die "Need at least " . MOST_DOCS . " documents; got $line_num" unless $line_num >= MOST_DOCS;

    push @docs, decode_json($smallest_doc);
    push @docs, decode_json($largest_doc);
    return $line_num, @docs;
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

my ($dataset_size, @data) = get_data_from_json($dataset_path, 2_000);
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
print "Dataset: " . file($dataset_path)->basename . ", size: $dataset_size\n";

# profile will only enable profiling if the user has requested it. Otherwise we
# try to execute the function in question with as little overhead as possible in
# order to get accurate benchmark times.
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
    $read_coll->insert($_) for @{$dataset{all_docs}};
    print " done.\n";
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

    timethese( -2, {

        "single insert" => sub {

            profile( sub { $insert_coll->insert($dataset{single_doc}) } );
        },

        "batch insert ".SOME_DOCS." docs" => sub {

            profile( sub { $insert_coll->insert($dataset{some_docs}) } );
        },

        "batch insert ".MOST_DOCS." docs" => sub {

            profile( sub { $insert_coll->insert($dataset{most_docs}) } );
        },

        "single insert small doc" => sub {

            profile( sub { $insert_coll->insert($dataset{smallest_doc}) } );
        },

        "batch insert ".SOME_DOCS." small doc" => sub {

            profile( sub { $insert_coll->insert(\@small_doc_some) } );
        },

        "batch insert ".MOST_DOCS." small doc" => sub {

            profile( sub { $insert_coll->insert(\@small_doc_most) } );
        },

        "single insert large doc" => sub {

            profile( sub { $insert_coll->insert($dataset{largest_doc}) } );
        },

        "batch insert ".SOME_DOCS." large doc" => sub {

            profile( sub { $insert_coll->insert(\@large_doc_some) } );
        },

        "batch insert ".MOST_DOCS." large doc" => sub {

            profile( sub { $insert_coll->insert(\@large_doc_most) } );
        }
    });

    $insert_coll->drop;
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

# GridFS
# This benchmark is independent of the dataset and as such uses a known dataset. We can get
# away with doing this here since we are only dealing with opqaue bytes.
{

    my $str_4kb = ('1') x 4096;
    my $str_500kb = ('1') x 512_000;
    open( my $fh, '<', \$str_4kb);
    open( my $fh2, '<', \$str_500kb);

    # Prepare for reads
    my $grid = $db->get_gridfs;
    $grid->insert($fh, {"filename" => "4kb"});
    $grid->insert($fh2, {"filename" => "500kb"});

    timethese( -2, {

        "insert 4kb file" => sub {

            open( my $tempfh, '<', \$str_4kb);
            profile( sub { $grid->insert($tempfh) } );
        },

        "insert 500kb file" => sub {

            open( my $tempfh, '<', \$str_500kb);
            profile( sub { $grid->insert($tempfh) } );
        },

        "find 4kb file" => sub {

            open( my $tempfh, '<', \$str_4kb);
            profile( sub { $grid->find_one({filename => "4kb"}) } );
        },

        "find 500kb file" => sub {

            open( my $tempfh, '<', \$str_500kb);
            profile( sub { $grid->find_one({filename => "500kb"}) } );
        },
    });
}

END {

    # Cleanup
    DB::finish_profile if $profile;
    $db->get_collection("read")->drop if $db;
}