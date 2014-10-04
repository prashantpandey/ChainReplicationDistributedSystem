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
var accDetails = {};

/* General functions */

/**
 * check whether the reqId is already been served
 *
 * @reqId: request Id to be checked
 */
function checkRequest(reqId) {
    if (reqId  in historyReq) {
        return true;
    }
    else {
        return false;
    }
}

// TODO: Phase 3
/**
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
}i

// TODO: Phase 3
/**
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

/**
 * Update the loacl history with the request payload
 * Returns "true" when the update is successful 
 * else returns "false"
 *
 * @payload: received payload
 */
function applyUpdate(payload) {
    var reqId = payload.transaction.reqId;
    if(!checkRequest(reqId)) {
        historyReq = {'reqId' : payload};
        accDetails[payload.transaction.accNum] = payload.result.currBal;
        return true;
    }
    else {
        logger.error("Request Inconsistent with history" + payload);
        return false;
    }
}

/**
 * add the payload to the sentReq list
 * to be used while handling the failures
 * When the ack is received from the tail, then all the requests 
 * smaller the ack'ed reqId will be deleted
 *
 * @payload: payload to be appended to the sentReq
 */
function appendSentReq(payload) {
    var reqId = payload.transaction.reqId;
    sentReq = {'reqId' : payload};
}

/**
 * fetch balance corresponding to the account number
 *
 * @accNum: account number
 */
function getBalance(accNum) {
    return accDetails[accNum];
}

/**
 * perform the transaction on the account
 *
 * @accNum: account number on which to perform the update
 * @amount: amount to be used in transaction
 * @oper: operation type
 */
function performUpdate(accNum, amount, oper) {
    switch(oper) :
        case Operation.Deposit:
            accDetails[accNum] = accDetails[accNum] + amount;
            return Outcome.Processed;
        case Operation.Withdraw:
            if(accDetails[accNum] < amount) {
                return Outcome.InsufficientFunds;
            }
            else {
                accDetails[accNum] = accDetails[accNum] - amount;
                return Outcome.Processed;
            }
        default:
            logger.error('Operation not permitted' + oper);
}

/**
 * generic function to send request to destination entity
 * destination could be client, master or some other server in chain
 *
 * @data: data to sent as body of the request
 * @dest: address(hostname:port) of the destination entity
 * @context: context info (who's invoking the function)
 */
function send(data, dest, context) {
    var options =
    {
        'host': dest.hostname,
        'port': dest.port,
        'path': '/',
        utcome
        'method': 'POST',
        'headers' : { 'Content-Type' : 'application/json',
                       'Content-Length' : 'chunked'
                    }
    };

    var req = http.request(options, function{
        var str = '';
        response.on('data', function(data){
            str += data;
        });
        response.on('end', function(){
            logger.info(context + ': Acknowledgement received' + str);
        });
    });

    req.write(JSON.stringify(data));
    req.on('error', function(e){
        logger.error(context + ': Problem occured while requesting' + e)
    });
    req.end();
}

/**
 * process the sync request from the predecessor server.
 *
 * @payload: payload recieved from the sync request
 */
function sync(payload) {
    // TODO: Implement the transfer logic to be implement at the tail server
    logger.info('Processing sync request: ' + payload);
    applyUpdate(payload);
   
    if(successorType == "Tail") {
        send(payload.result, payload.client, 'sendResponse');
        var ack = {
            'reqId' : reqId,
            'serverId' : serverId
        };
        send(ack, successor, 'sendAck');
    }
    else {
        appendSentReq(payload);
        send(payload, predecessor, 'sendSyncReq');
    }
    logger.info('Sync request processed');
}

/**
 * query the exiting account details for the balance
 * query is performed at the tail
 * the response is sent to the client
 * @payload: payload received in the query request
 */
function query(payload) {
    logger.info('Processing the query request: ' + payload);
    var accNum = payload.transaction.accNum;
    var bal = getBalance(accNum);
    if(bal == undefined) {
        logger.error('Account number not found' + accNum);
        logger.info('Creating a new account with the given account number');
        accDetails = {accNum : 0};
        bal = 0;
    }
    var response = {
        'outcome' : Outcome.Processed,
        'currBal' : bal
    };
    var dest = {
        'hostname' : payload.client.hostname;
        'port' : payload.client.port
    };
    send(response, dest, 'queryResponse');
    logger.info('Query request processed');
}

/**
 * perform the update transaction using the payload
 * also forward the sync request to the predecessor server
 * in the chain
 * the update is performed at the head server
 *
 * @payload: payload received in the upadate request
 */
function update(payload) {
    logger.info('Processing the update request ' + payload);
    var accNum = payload.transaction.accNum;
    var amount = payload.transaction.amount;
    var oper = payload.transaction.operation;

    var outcome = performUpdate(accNum, amount, oper);
    var currBal = getBalance(accNum);
    logger.info('Transaction Outcome: ' + outcome + 'Current Bal: ' + currBal);
    
    payload = {
        'result' : {
            'outcome' : outcome,
            'currBal' : currBal
        }
    };
    appendSentReq(payload);
    historyReq = {'reqId' : payload};
    send(payload, predecessor, 'sendSyncReq');
    logger.info('Processed the update request');
}

/**
 * handle acknowledgement from the predecessor server
 *
 * @reqId: request Id
 * @serverId: server Id of predecessor
 */
function handleAck(payload) {
    var reqId = payload.reqId;
    var serverId = payload.serverId;

    for(req in sentReq) {
        if(reqId < req.reqId) {
            sentReq.remove(req);
        }
    }
    send(payload, predecessor, 'sendSyncReq');
}

/**
 * check if the req has already been processed
 */
function checkLogs(payload) {
    var reqId = payload.reqId;
    var response = '';
    if(checkRequest(reqId)) {
        res = {'response' : 'true'};
    }
    else {
        res = {'response' : 'false'};
    }
    var dest = {
        'hostname' : payload.client.hostname;
        'port' : payload.client.port
    };
    send(response, dest, 'checkLogsResponse');
    logger.info('Check logs request processed');
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
            request.on('end', function() {
                // parse the msg body to JSON once it is fully received
                payload = JSON.parse(fullBody);

                // sequester the request based upon the element present
                // in the message body
                if(payload.sync) {
                    sync(payload); 
                }
                else if(payload.query) {
                    query(payload);   
                }
                else if(payload.update) {
                    update(payload);
                }
                else if (payload.failure) {
                    // TODO: Phase 3
                    handleChainFailure(payload);
                }
                else if(payload.ack) {
                    handleAck(payload);
                }
                else if(payload.checkLog) {
                    checklogs(payload)
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


// TODO: Phase 3
// Handle the heart beat signals to be sent to master node
/**
 * Setup the timer for regular heart beat signals
 */
// setInterval(sendHeartBeat, 5000);



