/*
 * Server side for Banking Application.
 * It implements Chain Replication algo
 */

/* Custom includes */
var syncMsgContext = require('./SyncMsgContext.js');
var reply = require('./Reply.js');
var request = require('./Request.js');
var logger = require('./Logger.js');

/* Config File include */
var config = require('./server_config.json');

/* System includes */
var http = require('http');
var sys = require('sys');
var fs = require('fs');
var winston = require('winston');

/* Data Structures */
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

var totalReqCount = 0;
var serverType;
var successor;
var predecessor;
var serverId;

var sentReq = {};
var historyReq = {};

/* general options object for making http request */
var options =
{
    'host': '',
    'port': '',
    'path': '/',
    'method': 'POST',
    'headers' : { 'Content-Type' : 'application/json',
                   'Content-Length' : 'chunked'
                }
};

/* General functions */

/*
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

/*
 * Send heart beat signals to master
 */
function sendHeartBeat() {
    var options = {
        hostname : config.master.hostname,
        port: config.master.port
    }
    var req = http.request(options, 
            function(response) {
                logger.info('Received ack back from master');
    });
    req.write(serverId);
}

/*
 * Check whether the MaxService limit is reached
 */
function checkMaxServiceLimit() {
    if(config.bank[0].servers[0].serverLifeTime != "UNBOUND") {
        if(totalReqCount >= config.bank[0].servers[0].serverLifeTime) {
            logger.info('Server request limit reached. Terminating server.');
            process.exit(0);
        }
    }
}

/*
 * process the sync request from the predecessor server.
 */
function sync(payload) {
    if(serverType == 'Tail' && payload.operation == 'Transfer') {
        var req = payload.request;
        var head = '';
        for(bank in config.bank) {
            if(payload.destBankId == bank.bankId) {
                head = bank.headServer;
            }
        }

        options.host = head.hostname;
        options.port = head.port;
        
        var destReq = {
            'reqId' : payload.reqId,

        };

        var destRes = '';
        var req = http.request(options, function(response) {
            var str = '';
            response.on('data', function(data){
                str += data;
            });

            response.on('end', function(){
               destRes = str;
            });
        });

        req.write(JSON.stringify(destReq));
        req.on('error', function(e){
            logger.error('Problem with the request message' + destReq);
        });
        req.end();
       
        applyUpdate(payload);

        if(successor != undefined) {
            sendSyncUpdate(payload);
        }
        else {
            sendResponse(payload);
        }
        
    }
}

/*
 * create the server and start it
 */
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
        
        if(request.method == 'POST') {
            // if it is a POST request then load the full msg body
            var fullBody ='';
            request.on('data',function(chunk) {
                fullBody += chunk;
            }); 
            var payload = '';   

            // parse the msg body to JSON once it is fully received
            request.on('end', function() {
                payload = JSON.parse(fullBody);
                if(payload.sync) {
                    // write the sync handling logic
                    logger.info(payload);
                }
                else if(payload.query) {
                    
                }
                else if(payload.update) {
                }
                else if (payload.failure) {
                }
                else if(payload.ack) {
                }
                else if(payload.checkLog) {
                }
                else {
                    logger.info('Unknown request payload: ' + fullBody);
                }
            });


        }
        response.write('Hello World...\n from the server..\n');
        response.end();
    }
);
server.listen(8000);
logger.info('Server running at http://127.0.0.1:8000/');


/**
 * Setup the timer for regular heart beat signals
 */
// setInterval(sendHeartBeat, 5000);



