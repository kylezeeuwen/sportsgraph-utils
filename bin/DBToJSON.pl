#!/usr/bin/perl

use strict;
use warnings;

use Data::Dump qw(dump);
use DBI;
use JSON qw(encode_json);
use Getopt::Long;

my $outFile = "/home/dev/projects/sportgraph/data/league.json";
my $filter;

GetOptions(
    "outfile:s" => \$outFile,
    "filter:s"  => \$filter
);
my $dsn = "dbi:mysql:database=nhl;host=localhost;port=3306";
my $user = "nhl";
my $pass = "nhl";

my $dbh = DBI->connect($dsn,$user,$pass, { RaiseError => 1, AutoCommit => 1 });

my $json = {
    seasons => $dbh->selectcol_arrayref(q{SELECT DISTINCT(season) FROM roster }),
    roster  => make_roster(),
    players => $dbh->selectall_hashref(q{SELECT * FROM player}, ['player_id']),
    arenas  => $dbh->selectall_hashref(q{SELECT * FROM arena}, ['team_id']), #XXX: Data model limitation
    teams   => $dbh->selectall_hashref(q{SELECT * FROM team}, ['team_id']),
};

open my $fh, '>', $outFile or die $!;
print $fh encode_json($json);
close $fh or die $!;

printf "Wrote league data to $outFile\n";

sub make_roster {
    my $where = '';
    if ($filter) {
        die "invalid filter $filter" unless $filter =~ /^[\w%]+$/;
        $where = "WHERE p.firstname LIKE '$filter'";
    }

    my $roster = $dbh->selectall_hashref(qq{
        SELECT r.season, r.team_id, GROUP_CONCAT(r.player_id) AS players 
        FROM roster r JOIN player p USING (player_id)
        $where 
        GROUP BY season, team_id
    }, ['season','team_id'], { Slice => {} });

    my $record_count = 0;
    for my $season (sort keys %$roster) {
        for my $team_id (sort keys %{$roster->{$season}}) {
            $roster->{$season}{$team_id} = [ split(/,/, $roster->{$season}{$team_id}{players}) ];
            $record_count += scalar(@{$roster->{$season}{$team_id}});
        }
    }
    print "League contains $record_count records\n";

    return $roster;
}
