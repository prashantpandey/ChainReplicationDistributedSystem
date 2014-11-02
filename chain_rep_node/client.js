/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */

var config = require('./config.json');
//var reqData = require('./inconsistentHistoryPayload.json');
//var reqData = require('./samePayload.json');
var reqData = require('./randomPayload.json');
// var reqData = require('./payload.json');
var logger = require('./logger.js');

/* System includes */
var Fiber = require('fibers');
var http = require('http');

var Outcome = {
    Processed: 0,
    InconsistentWithHistory: 1,
    InsufficientFunds: 2
};

var Operation = {
    GetBalance: 0,
    Deposit: 1,
    Withdraw: 2,
    Transfer: 3
};


var bankServerMap = {};
var responses = {};
var clientId = '';
var port = '';

/**
 * will prepare a map of bankId vs the head and tail server
 * this structure to be used while making query/update operation
 * to bank 
 * Also in case of a failure msg from Master, this structure will be updated
 */
function prepareBankServerMap() {
    logger.info('ClientId: ' + clientId  + ' Preparing the bank server map');
    for (var i = 0; i < config.bank.length; i++) {
        var serverDetails = {
            "headServer" : config.bank[i].headServer,
            "tailServer" : config.bank[i].tailServer
        };
        var id = config.bank[i].bankId;
        bankServerMap[id] = serverDetails;
        // logger.info('ClientId: ' + clientId  + ' Bank entry added: ' + id + ' ' + JSON.stringify(bankServerMap[id]));
    }
    // logger.info('ClientId: ' + clientId  + ' Added ' + i + ' entries to the bank details');
}

/**
 * function to perform operation on the bank servers
 *
 * @payload: which contains the request message body
 */
function performOperation(payload) {
    var bankId = payload.bankId;
    var opr = payload.operation;
    var reqId = payload.reqId;
    var data = {};
    var dest = {};

    if(opr == Operation.GetBalance) {
        data['query'] = payload;
        dest = bankServerMap[bankId].tailServer;
    }
    else if (opr == Operation.Withdraw || opr == Operation.Deposit) {
        data['update'] = payload;
        dest = bankServerMap[bankId].headServer;
    }
    logger.info('ClientId: ' + clientId  + ' Performing request ' + reqId + ' on bank: ' + bankId);
    logger.info('ClientId: ' + clientId  + ' Destination info: ' + JSON.stringify(dest));
    send(data, dest, 'client');   
}

/**
 *
 */
function sleep(ms) {        
    var fiber = Fiber.current;
    setTimeout(function() {
        fiber.run();
    }, ms);
    Fiber.yield();
}


/**
 * generic function to send request to destination entity
 * destination could be client, master or some other server in chain
 *
 * @payload: payload to sent as body of the request
 * @dest: address(hostname:port) of the destination entity
 * @context: context info (who's invoking the function)
 */
function send(payload, dest, context) {
    logger.info('ClientId: ' + clientId  + ' payload: ' + JSON.stringify(payload));

    var options =
    {
        'host': dest.hostname,
        'port': dest.port,
        'path': '/',
        'method': 'POST',
        'headers' : { 'Content-Type' : 'application/json'
                    }
    };

    var req = http.request(options, function(response) {
        var str = '';
        response.on('data', function(payload) {
            str += payload;
        });
        response.on('end', function() {
            var resBody = JSON.parse(str);
            // logger.info('ClientId: ' + clientId  + ' Received data: ' + str);
            if(payload.query) {
                logger.info('ClientId: ' + clientId  + ' Adding response to db');
                responses[resBody.reqId] = resBody;
                logger.info('ClientId: ' + clientId  + ' Responses: ' + JSON.stringify(responses));
            }
        });
    });

    req.write(JSON.stringify(payload));
    req.on('error', function(e){
        logger.error('ClientId: ' + clientId  + ' ' + context + ' Problem occured while requesting ' + e)
    });
    req.end();
}

/**
 * Server to receive responses from the tail
 *
 */
var server = http.createServer(function(request, response) {
    response.writeHead(200, {'Content-Type' : 'text/plain'});
    
    logger.info('ClientId: ' + clientId  + ' Response received from Server');
    if(request.method == 'POST') {
        var str = '';
        
        request.on('data', function(chunk) {
            str += chunk;
        });

        request.on('end', function() {
            var resBody = JSON.parse(str);
            // logger.info('ClientId: ' + clientId  + ' Received data: ' + str);
            logger.info('ClientId: ' + clientId  + ' Adding response to db');
            responses[resBody.reqId] = resBody;
            logger.info('ClientId: ' + clientId  + ' Responses: ' + JSON.stringify(responses));
        });
    }

});

var arg = process.argv.splice(2);
clientId = arg[0];
port = arg[1];
server.listen(port);
logger.info('ClientId: ' + clientId  + ' Client server running at http://127.0.0.1:' + port);

// prepare the mapping for the bank
prepareBankServerMap();

/**
 * Invoking the task function 
 * to perform update operation on the server
 */
// task();

/**
 * function by which client requests for bal, withdraw or transfer 
 * to different banks.
 * bal : query requests are sent at the tail server of the bank
 * withdraw/transfer : update requests are sent at the head server of bank
 */
Fiber(function() { 
    logger.info('Starting to perform query/update operations');
    var data = reqData.data;
    // logger.info('Data: ' + JSON.stringify(data));
    for( var i = 0; i < data.length; i++) {
        if(data[i].clientId == clientId) {
            var payloads = data[i].payloads;
            var len = data[i].payloads.length;
            var prevReqId = ''
            for(var j = 0; j < len; j++) {
                var payload = payloads[j].payload;
                var reqId = payload.reqId;
                logger.info('Processing request: ' + reqId);
                var nums = reqId.split(".");
                logger.info(nums[0] + ' ' + nums[1]);
                if(nums[1] > 1) {
                    logger.info('Waiting for response: ' + prevReqId);
                    if(responses[prevReqId]) {
                        performOperation(payload);
                        prevReqId = reqId;
                        continue;
                    }                    
                    else {
                        for(;!responses[prevReqId];) {
                            sleep(2000);
                        }
                        performOperation(payload);
                        prevReqId = reqId;
                        continue;
                    }
                    /*
                    if(responses[prevReq]) {
                        performOperation(payload);
                        continue;
                    }
                    var intervalObject = setInterval(function() {
                        if(responses[prevReq]) {
                            logger.info('Got the response to previous opr: ' + prevReq);
                            performOperation(payload);
                            clearInterval(intervalObject);
                        }
                        else {
                            logger.info('Still waiting for response for previous query: ' + prevReq);
                            logger.info('Current responses: ' + JSON.stringify(responses));
                        }
                    }, 3000);
                    */
                }
                else  {
                    performOperation(payload); 
                    prevReqId = reqId;
                }
            }
        }
    }
}).run();


