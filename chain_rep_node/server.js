
/*
 * Server side for Banking Application.
 * It implements Chain Replication algo
 */

var Outcome = {
    Processed: 0,
    InconsistentWithHistory: 1,
    InsufficientFunds: 2
}

var Operation = {
    GetBalance: 0,
    Deposit: 1,
    Withdraw: 2,
    Transfer: 3
}

var ServerRelation = {
    Successor: 0,
    Predecessor: 1
}

var ServerType = {
    Head: 0,
    Internal: 1,
    Tail: 2
}


var syncMsgContext = require('./SyncMsgContext.js');
var reply = require('./Reply.js');
var request = require('./Request.js');

var http = require('http');
var sys = require('sys');
var fs = require('fs');

var totalReqCount = 0;
var serverType;
var successor;
var predecessor;
var serverId;

var sentReq = {};
var historyReq = {};

var masterConfig = {
    hostname: 'localhost',
    port: '80',
    method: 'POST'
};

/**
 * check whether the reqId is already been served
 */
function checkRequest(reqId) {
    if (reqId  in historyReq) {
        return true;
    }
    else {
        return false;
    }
}


function sendHeartBeat() {
    var req = http.request(masterConfig, 
            function(response) {
                console.log('Received ack back from master');
    });
    req.write(serverId);
}


/**
 * load different parameter from the config file
 */

function loadConfigFile()


// create a server
var server = http.createServer(
    function(request, response) {
        response.writeHead(200, {'Content-Type': 'text/plain'});
        // call request handler
        // this function will handle all the events
        // 1. sync request
        // 2. query/update operation
        // 3. failure (internal or head/tail)
        // 4. acknowledgement
        // 5. checkLogs
        response.write('Hello World...\n from the server..\n');
    }
);

server.listen(8000);

console.log('Server running at http://127.0.0.1:8000/');

