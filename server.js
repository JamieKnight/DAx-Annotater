var async = require('async');
var Curl = require('node-curl');
var parse = require('csv-parse');
var fs = require('fs');
var csv = require("fast-csv");

//general variables
var curl_options = { 
    HTTPHEADER: 'Accept: application/json',  
    SSLCERT: './dev.bbc.co.uk.pem'
}

//URL elements.
var nitro = "https://api.live.bbc.co.uk/nitro/api/programmes/?";
var mixins = "&mixin=genre_groups&mixin=ancestor_titles";

//Collect the data from the CSV file
var parser = parse({delimiter: ','}, function(err, programmes){
    async.mapLimit(programmes, 10, fetchMetaData, function(err, progs){
        console.log(progs.length);
        var ws = fs.createWriteStream("my.csv");
        csv.write(progs, {headers: false}).pipe(ws);
    });
});

//Process the stream of CSV data.
fs.createReadStream('./pids.csv').pipe(parser);


/*
*   fetchMetaData
*
*   Load metadata from nitro based on the pid inside the label.
*   Returns callback with a new row merging CSV data with nitro data.
*/
function fetchMetaData(item, callback){
    
    var label = item[0].split('.');        
    var pid = label[label.length-2];
    
    console.log(pid);
    
    //callback(null, [pid]);
    
    var curl = Curl.create(curl_options);
    
    //could do more than one pid per pass, will leave for future complexity!
    curl(nitro + 'pid='+ pid + mixins, curl_options, function(err) {
        if (!err) {
            //parse json, merge item and episode data and return
            var episodes = JSON.parse(this.body).nitro.results.items;
            callback(null, merge(episodes[0], item));
        } else {
            callback(err, null); 
        } 
    });
} 

/*
*   Merge 
*   
*   Merge data from Niro (@ep) with data from the imported CSV (@item)
*/
function merge(ep, item){

    var row = {};
    
    row['name'] = item[0];
    row['pid'] = ep.pid;
    row['subcount'] = item[1];
    row['totalcount'] = item[2];
    row['percentage'] = item[3];

    if (typeof ep.genre_groups !== 'undefined' 
        && typeof ep.genre_groups.genre_group !== 'undefined' 
        && typeof ep.genre_groups.genre_group.genres !== 'undefined' 
        && typeof ep.genre_groups.genre_group.genres.genre !== "undefined" ) {
        
        if (typeof ep.genre_groups.genre_group.genres.genre.length == 'undefined') {
            row['level_one_genre'] = ep.genre_groups.genre_group.genres.genre.$;
        } else {
            row['level_one_genre'] = ep.genre_groups.genre_group.genres.genre[0].$
            row['level_two_genre'] = ep.genre_groups.genre_group.genres.genre[1].$  
        }
    }
    
    if (typeof ep.ancestor_titles !== 'undefined') {
        ep.ancestor_titles.forEach(function(a){
            if (a.ancestor_type == "series") {
                row['series_pid'] = a.pid;  
                row['series_title'] = a.title; 
            } else if (a.ancestor_type == "brand")  {
                row['brand_pid'] = a.pid;  
                row['brand_title'] = a.title; 
            }
        })
    } else {
        row['series_pid'] = "unknown";
        row['series_title'] = "unknown";
        row['brand_pid'] = "unknown";
        row['brand_title'] = "unknown";
    }
    
    return row;
}


