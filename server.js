var async = require('async');
var Curl = require('node-curl');

var curl_options = { 
    HTTPHEADER: 'Accept: application/json',  
    SSLCERT: './dev.bbc.co.uk.pem'
}
var nitro = "https://api.live.bbc.co.uk/nitro/api/programmes/?";
var mixins = "&mixin=genre_groups&mixin=ancestor_titles";

var pids = ['b04n1wqm', 'b04n6sm0'];


async.map(pids, fetchMetaData, function(err, results){
    console.log(results);
});

function fetchMetaData(pid, callback){

    var curl = Curl.create(curl_options);

    curl(nitro + 'pid='+ pid + mixins, curl_options, function(err) {
        var episodes = JSON.parse(this.body).nitro.results.items;
    
        var rows = [];
        
        episodes.forEach(function(ep){
            var row = [];

            row['pid'] = ep.pid;

            //add genres data
            if (typeof ep.genre_groups !== 'undefined' && typeof ep.genre_groups.genre_group.genres !== 'undefined' ) {
                
                if (typeof ep.genre_groups.genre_group.genres.genre.length == 'undefined') {
                    row['level_one_genre'] = ep.genre_groups.genre_group.genres.genre.$;
                } else {
                    row['level_one_genre'] = ep.genre_groups.genre_group.genres.genre[0].$
                    row['level_two_genre'] = ep.genre_groups.genre_group.genres.genre[1].$  
                }
            }
            
            //add series pid and title if present
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
            
            rows.push(row);
        })    
        if (!err) { 
            callback(null, rows);
        } else {
            callback(err, null); 
        } 
    });
}