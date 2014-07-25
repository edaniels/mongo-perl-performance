use 5.008;
use strict;
use warnings;
use IO::Handle qw//;

use Benchmark qw/:all/;
use JSON::XS;

use MongoDB;

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

my @tweets = get_data_from_json('twitter.json', 10_000);
my %dataset = create_dataset(\@tweets);
my %scheme = (

	find_spec => {source=>"web"}
);

my $client = MongoDB::MongoClient->new;
my $db = $client->get_database("benchdb");

# Fill db for read with dataset
{
	print "Importing dataset...";
	STDOUT->flush();

	my $read_coll = $db->get_collection("read");
	$read_coll->drop;
	$read_coll->insert($_) for @{$dataset{all}};
	print " done.\n";
}

{
	my $insert_coll = $db->get_collection("inserts");

	timethese( -2, {

		"single insert" => sub {

			$insert_coll->insert($dataset{single_doc});
		},

		"batch insert 100" => sub {

			$insert_coll->insert($dataset{docs_100});
		},

		"batch insert 1000" => sub {

			$insert_coll->insert($dataset{docs_1000});
		}
	});

	$insert_coll->drop;
}

{
	my $read_coll = $db->get_collection("read");

	timethese( -2, {

		"find_one simple" => sub { $read_coll->find_one; },

		"find_one query" => sub { $read_coll->find_one($scheme{find_spec}); },

		"find all and iterate" => sub { 
			my $cursor = $read_coll->find();
			() while $cursor->next;
		},

		"find on query and iterate" => sub { 
			my $cursor = $read_coll->find($scheme{find_spec});
			() while $cursor->next;
		}
	});
}

$db->get_collection("read")->drop;