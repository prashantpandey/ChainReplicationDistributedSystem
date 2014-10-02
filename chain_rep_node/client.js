
/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */

var http = require('http');

// make request object
var request = http.request(
    {
        'host': 'localhost',
        'port': '8000',
        'path': '/',
        'method': 'GET'
    }
);

// assign callbacks
request.on('response', 
    function(response) {
        console.log('Response status code: ' + response.statusCode);

        response.on('data', 
            function(data) {
                console.log('Body: ' + data);
            }
        );
    }
);

request.end();
