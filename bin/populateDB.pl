#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use JSON qw(decode_json);
use Getopt::Long;

my $rosterDataDir = "/home/dev/projects/sportgraph/data/scrape/";
my $arenasFile    = "/home/dev/projects/sportgraph/data/arenas.json";
my $dsn = "dbi:mysql:database=nhl;host=localhost;port=3306";
my $user = "nhl";
my $pass = "nhl";
my $drop;

GetOptions(
    "drop"      => \$drop, 
);

my $dbh = DBI->connect($dsn,$user,$pass, { RaiseError => 1, AutoCommit => 1 });
dropTables($dbh) if $drop;
createTables($dbh);
truncateTables($dbh);

########################
# Process Roster Files
opendir(my $dh, $rosterDataDir) or die "error on dir $rosterDataDir: $!";
my @files = readdir($dh);
closedir($dh) or die "error on dir $rosterDataDir: $!";

for my $file (sort @files) {
    next if $file =~ /^\./;
    open my $fh, '<', "$rosterDataDir/$file" or die "error on file $file: $!";
    my $json_string;
    eval {
        while (<$fh>) { $json_string .= $_; }
        close $fh or die "error on file $file: $!";
        my $roster = decode_json($json_string);

        my $team = $roster->{team};
        my $season = substr($roster->{season},0,4);
        my $players = $roster->{players};

        my $team_id = addTeam($team);
  
        for my $player_data (@$players) {
            addRoster($team_id, $season, $player_data); 
        }
        printf "Processed $season $team roster\n";              
    };
    if ($@) {
        print "Error processing file $file: $@";
    }
}

########################
# Process Arenas Files
open my $fh, '<', $arenasFile or die "error on file $arenasFile: $!";
my $arenas;
eval {
    my $json_string;
    while (<$fh>) { $json_string .= $_; }
    close $fh or die "error on file $arenasFile: $!";
    $arenas = decode_json($json_string);
};
if ($@) {
    print "Error processing file $arenasFile: $@";
}

for my $team (sort keys %$arenas) {
    my $arena_info = $arenas->{$team};
    $arena_info->{team} = $team;
    eval {
        addArena($arena_info);
    };
    if ($@) {
        print "Error processing arena record for team $team: $@";
    }
}

sub addArena {
    my $info = shift;

    my ($team_id) = $dbh->selectrow_array(q{
        SELECT team_id FROM team where teamname = ?
    }, {}, $info->{team});
    
    die "no team_id for $info->{team}" unless $team_id;

    my ($minSeason, $maxSeason) = $dbh->selectrow_array(q{
        SELECT MIN(season), MAX(season) FROM roster
    });

    $info->{start_season} ||= $minSeason;
    $info->{end_season}   ||= $maxSeason;


    $dbh->do(q{
        INSERT IGNORE INTO arena
        SET
            team_id = ?,
            start_season = ?,
            end_season   = ?,
            arena_name   = ?,
            latitude     = ?,
            longitude    = ?
    }, {},
        $team_id,
        $info->{start_season},
        $info->{end_season},
        $info->{arena},
        $info->{lat},
        $info->{long}
    );
}

sub addTeam {
    my $team = shift;

    my $team_id;

    ($team_id) = $dbh->selectrow_array(q{
        SELECT team_id FROM team where teamname = ?
    }, {}, $team);
    return $team_id if defined $team_id;

    $dbh->do(q{
        INSERT IGNORE INTO team
        SET teamname = ?
    }, {}, $team);

    ($team_id) = $dbh->selectrow_array(q{
        SELECT team_id FROM team where teamname = ?
    }, {}, $team);

    return $team_id;
}

sub addPlayer {
    my ($firstname,$lastname) = @_;

    my $player_id;

    ($player_id) = $dbh->selectrow_array(q{
        SELECT player_id FROM player where firstname = ? and lastname = ?
    }, {}, $firstname, $lastname);
    return $player_id if defined $player_id;

    $dbh->do(q{
        INSERT IGNORE INTO player
        SET 
            firstname = ?,
            lastname  = ?
    }, {}, $firstname, $lastname);

    ($player_id) = $dbh->selectrow_array(q{
        SELECT player_id FROM player where firstname = ? and lastname = ?
    }, {}, $firstname, $lastname);

    return $player_id;
}

sub addRoster {
    my ($team_id, $season, $player_data) = @_;

    my @names = split(/ /, $player_data->{name});
    my $firstname = $names[0];
    my $lastname = $names[-1];
    
    my $player_id = addPlayer($firstname,$lastname);

    $dbh->do(q{
        INSERT IGNORE INTO roster
        SET
            season      = ?,
            player_id   = ?,
            team_id     = ?,
            position    = ?,
            sweater     = ?
    }, {}, 
        $season,
        $player_id,
        $team_id,
        $player_data->{position},
        $player_data->{sweater},
    );
}

#################################
# subroutines
my @tables = qw(roster player team arena);

sub truncateTables {
    my ($dbh) = @_;
    print "Truncating tables\n";

    for my $table (@tables) {
        $dbh->do("TRUNCATE TABLE $table");
    }
}

sub dropTables {
    my ($dbh) = @_;
    print "Dropping tables\n";

    for my $table (@tables) {
        $dbh->do("DROP TABLE $table");
    }
}

sub createTables {
    my ($dbh) = @_;
    print "Creating tables\n";

    $dbh->do(<<EOF
CREATE TABLE IF NOT EXISTS `roster` (
    roster_id INT NOT NULL AUTO_INCREMENT,
    season    INT NOT NULL,
    player_id INT NOT NULL,
    team_id   CHAR(3) NOT NULL,
    position  CHAR(1) DEFAULT NULL,
    sweater    TINYINT DEFAULT NULL,
    PRIMARY   KEY (`roster_id`),
    UNIQUE    KEY (`season`,`player_id`,`team_id`),
    KEY       `idx_team_season` (`team_id`, `season`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
EOF
    ) or die $!;

    $dbh->do(<<EOF
CREATE TABLE IF NOT EXISTS `player` (
    player_id     INT NOT NULL AUTO_INCREMENT,
    firstname     VARCHAR(50) NOT NULL,
    lastname      VARCHAR(50) NOT NULL,
    hometown      VARCHAR(50) NOT NULL,
    dob            INT(1) NOT NULL DEFAULT 1,
    latitude       DECIMAL(8,5) DEFAULT NULL,
    longitude      DECIMAL(8,5) DEFAULT NULL,
    PRIMARY        KEY (`player_id`),
    UNIQUE         KEY (`firstname`, `lastname`, `dob`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
EOF
    ) or die $!;

    $dbh->do(<<EOF
CREATE TABLE IF NOT EXISTS `team` (
    team_id     INT NOT NULL AUTO_INCREMENT,
    shortname   CHAR(3) DEFAULT NULL,
    teamname    VARCHAR(50) NOT NULL,
    color       VARCHAR(10) DEFAULT NULL,
    PRIMARY     KEY (`team_id`),
    UNIQUE      KEY (`shortname`), 
    UNIQUE      KEY (`teamname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
EOF
    ) or die $!;

    $dbh->do(<<EOF
CREATE TABLE IF NOT EXISTS `arena` (
    arena_id        INT NOT NULL AUTO_INCREMENT,
    team_id         INT NOT NULL,
    start_season    INT NOT NULL,
    end_season      INT NOT NULL,
    arena_name      VARCHAR(30) NOT NULL,
    latitude        DECIMAL(8,5) DEFAULT NULL,
    longitude       DECIMAL(8,5) DEFAULT NULL,
    PRIMARY         KEY (`arena_id`), 
    UNIQUE          KEY (`team_id`,`start_season`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
EOF
    ) or die $!;
}

