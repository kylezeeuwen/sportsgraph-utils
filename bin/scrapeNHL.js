var nhl  = require("/home/dev/projects/sportsgraph-alpha/node_modules/nhl-api/lib/nhl.js");
var fs   = require('fs');
var util = require('util');

var delay = 30000;
var baseDir = "/home/dev/projects/sportsgraph-alpha/data/scrape-201308/";

var teams = [
  'blackhawks',
  'bluejackets',
  'redwings',
  'predators',
  'blues',
  'flames',
  'avalanche',
  'oilers',
  'wild',
  'canucks',
  'ducks',
  'stars',
  'kings',
  'coyotes',
  'sharks',
  'devils',
  'islanders',
  'rangers',
  'flyers',
  'penguins',
  'bruins',
  'sabres',
  'canadiens',
  'senators',
  'mapleleafs',
  'hurricanes',
  'panthers',
  'lightning',
  'capitals',
  'jets'
];

var seasons = [
  '19951996',
  '19961997',
  '19971998',
  '19981999',
  '19992000',
  '20002001',
  '20012002',
  '20022003',
  '20032004',
  '20042005',
  '20052006',
  '20062007',
  '20072008',
  '20082009',
  '20092010',
  '20102011',
  '20112012',
  '20122013'
];

var seasonTypes = [
    'regular',
    'playoff'
];

var jobs = [];
for (var i = 0; i < teams.length; i++) {
  for (var j = 0; j < seasons.length; j++) {
    for (var k = 0; k < seasonTypes.length; k++) {
        
        var job = { team : teams[i], season : seasons[j], seasonType : seasonTypes[k] };
        var fileName = 
            baseDir + job.team + 
            "-" + job.season + 
            "-" + job.seasonType;

        if (!(fs.existsSync(fileName))) {
            jobs.push({ team : teams[i], season : seasons[j], seasonType : seasonTypes[k] });
        }
        else {
            util.log(util.format("Skipping team %s season %s type %s: file exists",
                job.team, job.season, job.seasonType
            ));
        }
    }
  }
}   

var intervalID = setInterval( function() {
    if (jobs.length > 0) {
    	var job = jobs.pop();
        util.log(util.format("Starting attempt for team %s season %s type %s", 
            job.team, job.season, job.seasonType
        )); 

        var fileName = 
            baseDir + job.team + 
            "-" + job.season + 
            "-" + job.seasonType;

        nhl.team(job, function(outArgs,players) {
          if(players) {
        
            var fileName = 
                baseDir + outArgs.team + 
                "-" + outArgs.season + 
                "-" + outArgs.seasonType;
        
            var struct = {
                'team' : outArgs.team,
                'season' : outArgs.season,
                'seasonType' : outArgs.seasonType,
                'players' : players
            };
        
            fs.writeFile(fileName, JSON.stringify(struct), function(err) {
              if(err) {
                util.log(util.format("Fail to save file for team %s season %s type %s: %s",
                  outArgs.team, outArgs.season, outArgs.seasonType, err
                ));
              } else {
                util.log(util.format("The file for team %s season %s type %s saved",
                  outArgs.team, outArgs.season, outArgs.seasonType
                ));
              }
            }); 
          }
          else {
            util.log(util.format("No players for team %s season %s type %s",
                outArgs.team, outArgs.season, outArgs.seasonType
            ));
          }
        });
    }
    else {
        clearInterval(intervalID);
    }
}, delay);

