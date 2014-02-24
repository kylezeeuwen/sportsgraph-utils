#!/usr/bin/perl

use strict;
use warnings;

use Data::Dump qw(dump);
use DBI;
use JSON qw(encode_json);
use Getopt::Long;

my $pretend = 1;

GetOptions(
    "pretend:i" => \$pretend,
);
my $dsn = "dbi:mysql:database=nhl;host=localhost;port=3306";
my $user = "nhl";
my $pass = "nhl";

my $dbh = DBI->connect($dsn,$user,$pass, { RaiseError => 1, AutoCommit => 1 });

my $player_histories = $dbh->selectall_arrayref(qq{
    select 
        player_id, group_concat(concat(season,'-',team_id))  history
    from 
        roster 
    group by player_id
}, { Slice => {} });

my $total = 0;
my $deleted = 0;
for my $row (@$player_histories) {
    $total++;
    my $p_id = $row->{player_id};
    my $history = [ sort split(/,/,$row->{history}) ];
    for (my $i = 0; $i < scalar(@$history) - 1; $i++) {
        my ($curS,$curT) = split(/-/,$history->[$i]); 
        my ($nextS,$nextT) = split(/-/,$history->[$i + 1]);

        if ($curS eq $nextS) {
            #printf("Dupe! P_ID %s History %s\n", $p_id, $row->{history});

            my ($deleteS, $deleteT);
            if ($i > 0) {
                my ($prevS,$prevT) = split(/-/,$history->[$i - 1]);
                ($deleteS,$deleteT) = ($prevT eq $curT) 
                    ? ($nextS,$nextT) 
                    : ($curS,$curT);
            }
            elsif ($i + 2 < scalar(@$history)) {
                my ($nextnextS,$nextnextT) = split(/-/,$history->[$i + 2]);
                ($deleteS,$deleteT) = ($nextT eq $nextnextT) 
                    ? ($curS,$curT)
                    : ($nextS,$nextT); 
            }
            else {
               #printf("Duplicate detected p_id %s %s %s. Picking second team\n",
               #    $p_id, $history->[$i], $history->[$i + 1], $deleteS,$deleteT
               #);
                ($deleteS,$deleteT) = ($nextS,$nextT);
            }

            if ($deleteS && $deleteT) {
               # printf("Duplicate detected p_id %s %s %s. Choosing to delete %s %s\n",
               #     $p_id, $history->[$i], $history->[$i + 1], $deleteS,$deleteT
               # );
                $dbh->do(qq{
                    DELETE FROM roster
                    WHERE 
                        player_id = ? AND
                        season = ? AND
                        team_id = ?
                }, {}, $p_id, $deleteS, $deleteT) unless $pretend;
                $deleted++;
            }
        } 
    }
}

printf("Total records considered: %s. Total records deleted: %s\n", $total, $deleted);
