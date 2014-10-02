
/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */

var http = require('http');


var data = {
    'name' : [
        { 'fname' : 'prashant', 'lname' : 'pandey'},
        { 'fname' : 'kavita', 'lname' : 'agarwal'}
    ]
};
       
var payload = JSON.stringify(data);

// make request object
var options = 
{
    'host': 'localhost',
    'port': '8000',
    'path': '/',
    'method': 'POST',
    'headers' : { 'Content-Type': 'application/json',
                  'Content-Length': payload.length
                }
};


// assign callbacks

callback = function(response) {
    var str = ''

        response.on('data', function(data) {
                str += data;                
        });

        response.on('end', function() {
            console.log(str);
        });
}

var req = http.request(options, callback);


req.write(payload);

req.on('error', function(e){
    console.log('problem with the log' + data);
});

req.end();
