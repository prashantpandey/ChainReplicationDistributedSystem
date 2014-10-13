/**
 * Server side for Banking Application.
 * It implements Chain Replication algo
 */

/* Custom includes */
var syncMsgContext = require('./SyncMsgContext.js');
var reply = require('./Reply.js');
var request = require('./Request.js');
var logger = require('./logger.js');
var util = require('./util.js');

/* Config File include */
var config = require('./config.json');

/* System includes */
var http = require('http');
var sys = require('sys');
var fs = require('fs');

/* Data Structures */
var Outcome = {
    Processed: 0,
    InconsistentWithHistory: 1,
    InsufficientFunds: 2,
    InTransit: 3
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
var serverType = '';
var successor = {};
var predecessor = {};
var serverId = '';
var hostname = '';
var port = '';
var serverLifeTime = '';
var serverStartupDelay = '';

var sentReq = {};
var historyReq = {};
var accDetails = {};

/* General functions */

function loadServerConfig(bId, sId) {
    var details = util.parseServerInfo(bId, sId);
    // logger.info('Fetched details using util: ' + JSON.stringify(details));
    bankId = bId;
    serverId = sId;
    hostname = details.hostname;
    port = details.port;
    serverType = details.type
    serverLifeTime = details.serverLifeTime;
    serverStartupDelay = details.serverStartupDelay;
    // logger.info('ServerId: '+ serverId + ' Successor: ' + JSON.stringify(details.successor) + ' Predecessor: ' + JSON.stringify(details.predecessor));
    successor = details.successor;
    predecessor = details.predecessor;
}

/**
 * check whether the reqId is already been served
 *
 * @reqId: request Id to be checked
 */
function checkRequest(reqId) {
    return historyReq[reqId];
    /*
    if (reqId in historyReq) {
        return true;
    }
    else {
        return false;
    }
    */
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
}

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
    var reqId = payload.reqId;
    // logger.info('ServerId: '+ serverId + ' history: ' + JSON.stringify(historyReq) + ' RequestId: ' + reqId);
    // logger.info('ServerId: '+ serverId + ' payload for sync req: ' + JSON.stringify(payload));
    var accNum = payload.payload.update.accNum;
    if(!checkRequest(reqId)) {
        historyReq[reqId] = payload.payload;
        accDetails[accNum] = payload.currBal;
        return true;
    }
    else {
        logger.error("Request Inconsistent with history" + JSON.stringify(payload));
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
    var reqId = payload.reqId;
    sentReq[reqId] = payload.payload;
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
    // logger.info('ServerId: '+ serverId + ' Performing update opr ' + accNum + ' ' + amount + ' ' + oper +' ' + accDetails[accNum] + ' ' + getBalance(accNum));
    logger.info('ServerId: '+ serverId + ' Account info: ' + JSON.stringify(accDetails));
    switch(oper) {
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
            return Outcome.InconsistentWithHistory;
    }
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
    logger.info(serverId + ' ' + JSON.stringify(dest) + ' ' + context);
    var options =
    {
        'host': dest.hostname,
        'port': dest.port,
        'path': '/',
        'method': 'POST',
        'headers' : { 'Content-Type' : 'application/json'}
    };

    var req = http.request(options, function(response) {
        var str = '';
        response.on('data', function(data) {
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
    logger.info('ServerId: '+ serverId + ' Processing sync request: ' + JSON.stringify(payload));
    var reqId = payload.reqId;
    applyUpdate(payload);
   
    if(serverType == 2) {
        var dest = {
            'hostname' : payload.payload.update.hostname,
            'port' : payload.payload.update.port
        };
        var response = {
            'reqId' : payload.reqId,
            'outcome' : payload.outcome,
            'currBal' : payload.currBal,
            'accNum' : payload.accNum
        };
        send(response, dest, 'sendResponse');
        
        var ack = {
            'ack' : 1,
            'reqId' : reqId,
            'serverId' : serverId
        };
        send(ack, predecessor, 'sendAck'); 
    }
    else {
        appendSentReq(payload);
        send(payload, successor, 'sendSyncReq');
    }
    var response = {
        'genack' : 1,
        'reqId' : reqId,
        'outcome' : Outcome.Processed
    };
    logger.info('ServerId: '+ serverId + ' Sync request processed');
    logger.info('ServerId: '+ serverId + ' acc details: ' + JSON.stringify(accDetails));
    return response;
}

/**
 * query the exiting account details for the balance
 * query is performed at the tail
 * the response is sent to the client
 *
 * @payload: payload received in the query request
 */
function query(payload) {
    logger.info('ServerId: '+ serverId + ' Processing the query request: ' + JSON.stringify(payload));
    var reqId = payload.query.reqId
    var accNum = payload.query.accNum;
    var bal = getBalance(accNum);
    if(bal == undefined) {
        logger.error('ServerId: '+ serverId + ' Account number not found: ' + accNum);
        logger.info('ServerId: '+ serverId + ' Creating a new account with the given account number');
        accDetails[accNum] = 0;
        bal = 0;
    }
    // logger.info('ServerId: '+ serverId + ' Account info: ' + JSON.stringify(accDetails));
    var response = {
        'reqId' : reqId,
        'outcome' : Outcome.Processed,
        'currBal' : bal,
        'accNum' : accNum
    };
    logger.info('ServerId: '+ serverId + ' Query request processed: ' + JSON.stringify(response));
    return response;
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
    logger.info('ServerId: '+ serverId + ' Processing the update request ' + JSON.stringify(payload));
    var reqId = payload.update.reqId;
    var accNum = payload.update.accNum;
    var amount = payload.update.amount;
    var oper = payload.update.operation;
    var currBal = getBalance(accNum);
    
    if(currBal == undefined) {
        logger.error('ServerId: '+ serverId + ' Account number not found: ' + accNum);
        logger.info('ServerId: '+ serverId + ' Creating a new account with the given account number');
        accDetails[accNum] = 0;
        bal = 0; 
    }
    
    var outcome = performUpdate(accNum, amount, oper);
    currBal = getBalance(accNum);
    logger.info('ServerId: '+ serverId + ' Transaction Outcome: ' + outcome + ' Current Bal: ' + currBal);
    
    var response = {
        'sync' : 1,
        'reqId' : reqId,
        'outcome' : outcome,
        'currBal' : currBal,
        'accNum' : accNum,
        'payload' : payload
    };

    appendSentReq(payload);
    historyReq[reqId] = payload;
    logger.info('ServerId: '+ serverId + ' Processed the update request');
    return response;
}

/**
 * handle acknowledgement from the predecessor server
 *
 * @reqId: request Id
 * @serverId: server Id of predecessor
 */
function handleAck(payload) {
    logger.info('ServerId: '+ serverId + ' Processing the acknowledgement ' + JSON.stringify(payload));
    var reqId = payload.reqId;

    for(req in sentReq) {
        if(reqId < req.reqId) {
            sentReq.remove(req);
        }
    }
    if(serverType != 0) {
        send(payload, predecessor, 'ack');
    }
    var response = {
        'genack' : 1,
        'reqId' : reqId,
        'outcome' : Outcome.Processed
    };
    logger.info('ServerId: '+ serverId + ' Processed the acknowledgement');
    return response;
}

/**
 * check if the req has already been processed
 */
function checkLogs(payload) {
    var reqId = payload.reqId;
    var response = '';
    if(checkRequest(reqId)) {
        response = {
            'reqId' : reqId,
            'response' : 'true'
        };
    }
    else {
        response = {
            'reqId' : reqId,
            'response' : 'false'
        };
    }
    logger.info('Check logs request processed');
    return response;
}

var arg = process.argv.splice(2);
logger.info('Retrieved cmd line args: ' + arg[0] + ' ' + arg[1]);
loadServerConfig(arg[0], arg[1]);

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
        
        logger.info('ServerId: '+ serverId + ' Request received');
        if(request.method == 'POST') {
            var fullBody ='';
            var res = {};
            
            // if it is a POST request then load the full msg body
            request.on('data', function(chunk) {
                // logger.info('got data');
                fullBody += chunk;
            });

            request.on('end', function() {
                // logger.info('got end');

                // parse the msg body to JSON once it is fully received
                var payload = JSON.parse(fullBody);

                logger.info(payload);
                // sequester the request based upon the element present
                // in the message body
                
                if(payload.sync) {
                    res['result'] = sync(payload); 
                }
                else if(payload.query) {
                    res = query(payload);   
                }
                else if(payload.update) {
                    syncRes = update(payload);
                    send(syncRes, successor, 'sendSyncReq');
                    res['result'] = Outcome.InTransit;
                }
                else if (payload.failure) {
                    // TODO: Phase 3
                    handleChainFailure(payload);
                }
                else if(payload.ack) {
                    res['result'] = handleAck(payload);
                }
                else if(payload.checkLog) {
                    res = checklogs(payload)
                }
                else if (payload.genack){
                    logger.info('Gen request payload: ' + fullBody);
                }
                if(!payload.sync) {
                    logger.info('Response: ' + JSON.stringify(res)); 
                    response.end(JSON.stringify(res));
                }
                else {
                    reposnse.end();
                }
            });
        }
    }
);
server.listen(port);
logger.info('Server running at http://127.0.0.1:' + port);


// TODO: Phase 3
// Handle the heart beat signals to be sent to master node
/**
 * Setup the timer for regular heart beat signals
 */
// setInterval(sendHeartBeat, 5000);



