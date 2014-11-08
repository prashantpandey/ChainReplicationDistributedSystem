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
var config;

/* System includes */
var http = require('http');
var sys = require('sys');
var Fiber = require('fibers');

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

var serverType = '';
var successor = {};
var predecessor = {};
var serverId = '';
var hostname = '';
var port = '';
var bankId = '';
var serverLifeTime = {};
var serverStartupDelay = '';
var heartBeatDelay;

var sentReq = [];
var historyReq = {};
var accDetails = {};
var lastSentReq = '';
var totalSentCnt = 0;
var totalRecvCnt = 0;
var globalSeqNum = 0;

/* General functions */

function loadServerConfig(bId, sId) {
    config = require('./config.json');
    // var config = require('./config_headFailure.json');
    // var config = require('./config_tailFailure.json');
    heartBeatDelay = config.master.heartBeatDelay;
    
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

/**
 * Check whether the MaxService limit is reached
 */
function checkMaxServiceLimit() {
    if(serverLifeTime.RecvNum && serverLifeTime.RecvNum == totalRecvCnt) {
        logger.info('ServerId: '+ serverId + ' RECV request limit reached. Terminating server.');
        process.exit(0);
    }
    else if(serverLifeTime.SendNum && serverLifeTime.SendNum == totalSentCnt) {
        logger.info('ServerId: '+ serverId + ' SEND request limit reached. Terminating server.');
        process.exit(0);
    }
}

/**
 * Update the local history with the request payload
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
        historyReq[reqId] = {
	    'payload' : payload.payload,
	    'response' : payload
	};
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
    sentReq.push(payload);
    lastSentReq = reqId;
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
    logger.info("ServerId: " + serverId + ' ' + JSON.stringify(dest) + ' ' + context);
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
            logger.info("ServerId: " + serverId + ' ' + context + ': Acknowledgement received' + str);
        });
    });

    req.write(JSON.stringify(data));
    req.on('error', function(e){
        logger.error("ServerId: " + serverId + ' ' + context + ': Problem occured while requesting' + e)
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
        if(payload.payload.update.simFail == 2) {
            // response NOT SENT
            // this will simulate the failure condition
            // the packet dropped on the server <--> client channel
            logger.info('ServerId: '+ serverId + ' Simulating msg failure between Server-Client');
        }
        else {
            send(response, dest, 'sendResponse');
            totalSentCnt++;
        }
        
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
        totalSentCnt++;
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

    if(historyReq[reqId]) {
        var history = historyReq[reqId];
	logger.info('ServerId: '+ serverId + ' history: ' + JSON.stringify(history));
        if(history.payload.query.accNum == accNum) {
            var response = history.response;
            logger.info('ServerId: '+ serverId + ' Query request already exists in history: ' + JSON.stringify(response));
            return response;
        }
        else {
            var response = {
                'reqId' : reqId,
                'outcome' : Outcome.InconsistentWithHistory,
                'currBal' : 0,
                'accNum' : accNum
            };
            logger.info('ServerId: '+ serverId + ' Query request Inconsistent with history: ' + JSON.stringify(response));
            return response;
        }
    }
    
    var bal = getBalance(accNum);
    if(bal == undefined) {
        logger.error('ServerId: '+ serverId + ' Account number not found: ' + accNum);
        logger.info('ServerId: '+ serverId + ' Creating a new account with the given account number');
        // don't create an account on tail, if one doesn't exist
        // accDetails[accNum] = 0;
        bal = 0;
    }
    // logger.info('ServerId: '+ serverId + ' Account info: ' + JSON.stringify(accDetails));
    var response = {
        'reqId' : reqId,
        'outcome' : Outcome.Processed,
        'currBal' : bal,
        'accNum' : accNum
    };
    
    //  add the payload and response to historyReq
    var history = {
        'payload' : payload,
	'response' : response    
    };

    historyReq[reqId] = history;

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

    if(historyReq[reqId]) {
        var history = historyReq[reqId];
	logger.info('ServerId: '+ serverId + ' history: ' + JSON.stringify(history));
        if(history.payload.update.accNum == accNum && history.payload.update.amount == amount && history.payload.update.operation == oper) {
            var response = history.response;
            delete response['payload'];
            delete response['sync'];
            logger.info('ServerId: '+ serverId + ' Updated response: ' + JSON.stringify(history.response));
            logger.info('ServerId: '+ serverId + ' Update request already exists in history: ' + JSON.stringify(response));
            return response;
        }
        else {
            var response = {
                'reqId' : reqId,
                'outcome' : Outcome.InconsistentWithHistory,
                'currBal' : 0,
                'accNum' : accNum
            };
            logger.info('ServerId: '+ serverId + ' Update request Inconsistent with history: ' + JSON.stringify(response));
            return response;
        }
    }

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
        'reqId' : reqId,
        'outcome' : outcome,
        'currBal' : currBal,
        'accNum' : accNum,
        'payload' : payload,
        'sync' : 1
    };
 
    // add the payload and response to historyReq
    var history = {
        'payload' : payload,
        'response' : response    
    };

    appendSentReq(response);
    historyReq[reqId] = history;

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
    var i = 0;
    logger.info(sentReq.length);
    for(i = 0; i < sentReq.length; i++) {
	logger.info('SentReq: ' + JSON.stringify(sentReq[i]));
	if(reqId == sentReq[i].reqId) {
	    break;    
	}
    }
    sentReq.splice(0, i + 1);
    /*
    var nums = reqId.split('.');
    for(i = 0; i < nums[1]; i++) {
        key = nums[0] + '.' + i;
        if(sentReq[key]) {
            delete sentReq[key]
            // sentReq.remove(key);
        }
    }
    */
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
 * handle the failure use case
 * update the server type in case of head/tail failure
 * update the succ/pred in case of internal failure
 */
function handleChainFailure(payload) {
    logger.info('ServerId: '+ serverId + ' handling the server failure');
    var server = payload.failure.server;
    var type = payload.failure.type;
    if(type == 'head') {    // change server type
        serverType = 0;
	logger.info('ServerId: '+ serverId + ' updated the server type to HEAD');
    }
    else if (type == 'tail') {   // change server type
        serverType = 2;
	logger.info('ServerId: '+ serverId + ' updated the server type to TAIL');
    }
    else if(type == 'successor') {   // change successor: this is pred
	successor = server;
	logger.info('ServerId: '+ serverId + ' updated the successor server');
	handleNewSucc(payload.failure.seqNum);
	var payload = {                 // this is just a place holder to avoid null error at master
		'seqNum' : lastSentReq  // nothing specific to logic
	};                              // don't take it seriously :)
	return payload;
    }
    else if(type == 'predecessor') {    // change predecessor: this is succ
	predecessor = server;
	var payload = {
		'seqNum' : lastSentReq
	};
	logger.info('ServerId: '+ serverId + ' updated the predecessor server');
	return payload;
    }
}

/**
 * handle internal server failure and resolve
 * sentReq anomalies by synchronizing the sentReq
 */
function handleNewSucc(lastSeqSucc) {
    logger.info('ServerId: '+ serverId + ' sending sync requests to the new successor');
    var flag = false;
    var i = 0;
    for(i = 0; i < sentReq.length; i++) {
	logger.info(JSON.stringify(sentReq[i]));
	if(sentReq[i].reqId == lastSeqSucc) {
	    flag = true;
	    break;
	}
    }
    if(!flag) {
	for(i = 0; i < sentReq.length; i++) {
	    var reqId = sentReq[i].reqId;
	    logger.info(JSON.stringify(successor) + ' ' +JSON.stringify(historyReq[reqId].response));
	    send(historyReq[reqId].response, successor, 'sendSyncReq');   
	}
    }
    else {
	for(var j = i; j < sentReq.length; j++) {
	    var reqId = sentReq[j].reqId;
	    // logger.info(reqId);
	    logger.info(JSON.stringify(successor) + ' ' +JSON.stringify(historyReq[reqId].response));
	    send(historyReq[reqId].response, successor, 'sendSyncReq');   
	}
    }
    logger.info('ServerId: '+ serverId + ' sync requests sent');
}

/**
 * handle the extend chain functionality of the server
 * NewTail: Either set ack the master of getting added to the chain
 * OldTail: Or update the new successor and send the sync to the new tail
 *
 * @payload: attributes received from master
 */
function handleExtendChain(payload) {
    logger.info('ServerId: '+ serverId + ' Processing extend chain');
    if(payload.extendChain == 3) {
	logger.info('ServerId: '+ serverId + ' Starting to sync with the old tail');
	accDetails = payload.accDetails;
	sentReq = payload.sentReq;
	historyReq = payload.historyReq;
	logger.info(JSON.stringify(accDetails));
	logger.info(JSON.stringify(historyReq));
	logger.info(JSON.stringify(sentReq));
	logger.info('ServerId: '+ serverId + ' Sync completed with the old tail');
	send( {'ack' : 2 }, config.master, 'SyncComplete');
    }
    else if(payload.extendChain == -1) {    // extend chain failed
	serverType = 2;
	logger.info('ServerId: '+ serverId + ' Old tail reverted back');	
    }
    else if(payload.type == 2) {	    // its the new tail
	serverType = 2;
	predecessor = payload.predecessor;
	logger.info('ServerId: '+ serverId + ' Activating new tail and updating the predecessor');
	return { 'ack' : 1 };
    }
    else if(payload.type == 1) {    // its the old tail
	logger.info('ServerId: '+ serverId + ' Updating new successor and sync data with new tail');
	serverType = 1;
	successor = payload.successor;
	// sync the DB i.e. accDetails
	// sync the history
	// sync the sentReq as sync requests
	var data = {
	    'extendChain' : 3,
	    'accDetails' : accDetails,
	    'sentReq' : sentReq,
	    'historyReq' : historyReq,
	};
	logger.info(JSON.stringify(accDetails));
	logger.info(JSON.stringify(historyReq));
	logger.info(JSON.stringify(sentReq));
	logger.info('ServerId: '+ serverId + ' Updated new successor and sync data with new tail');
	send(data, successor, 'extendChain');
    }
    logger.info('ServerId: '+ serverId + ' processed extend chain request');
}

/**
 * check if the req has already been processed
 */
function checkLogs(payload) {
    logger.info('ServerId: '+ serverId + ' Processing check Logs request');
    var reqId = payload.reqId;
    var history = checkRequest(reqId);
    var response = {};
    if(history) {
	var response = history['response'];
	response['checkLog'] = 1;
    }
    else {
        response = {
            'checkLog' : 0,
            'reqId' : reqId,
        };
    }
    logger.info('ServerId: '+ serverId + ' Check logs request processed');
    logger.info(JSON.stringify(response));
    return response;
}

function contactMaster(payload) {
    logger.info('ServerId: '+ serverId + ' contacting master to extend chain');
    var data = { 'extendChain' : payload };
    send(data, config.master, 'AddToChain');
    logger.info('ServerId: '+ serverId + ' Extend chain done');
}

var arg = process.argv.splice(2);
logger.info('ServerId: '+ arg[1] + ' Retrieve cmd line args: ' + arg[0] + ' ' + arg[1]);

if(arg[2]) {
    logger.info('ServerId: '+ arg[1] + ' Got the the new server config arg');
    config = require(arg[2]);
    heartBeatDelay = config.master.heartBeatDelay;
    bankId = arg[0];
    serverId = arg[1];
    hostname = config.server.hostname;
    port = config.server.port;
    serverLifeTime = config.server.serverLifeTime;
    serverType = config.server.type;
    
    contactMaster(config.server);
}
else {
    loadServerConfig(arg[0], arg[1]);
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
        
        // logger.info('ServerId: '+ serverId + ' Request received');
        if(request.method == 'POST') {
            var fullBody ='';
            var res = {};
	    var flag = false;
            
            // if it is a POST request then load the full msg body
            request.on('data', function(chunk) {
                // logger.info('got data');
                fullBody += chunk;
            });

            request.on('end', function() {
                // logger.info('got end');

                // parse the msg body to JSON once it is fully received
                var payload = JSON.parse(fullBody);

                // logger.info(payload);
                // sequester the request based upon the element present
                // in the message body
                
                if(payload.sync) {
                    totalRecvCnt++;
                    res['result'] = sync(payload);
		    // flag = true;     // ommitting gen ack msg, due to flooding 
                }
                else if(payload.query) {
                    totalRecvCnt++;
                    res = query(payload);
                    if(payload.query.simFail == 2) {
                        // response NOT SENT
                        // this will simulate the failure condition
                        // the packet dropped on the server <--> client channel 
                        logger.info('ServerId: '+ serverId + ' Simulating msg failure between Server-Client');
                    }
                    else {
		        flag = true;
                    }
                }
                else if(payload.update) {
                    totalRecvCnt++;
                    syncRes = update(payload);
                    logger.info('ServerId: '+ serverId + ' Update req response: ' + JSON.stringify(syncRes));
                    if(syncRes.sync) {
                        logger.info('ServerId: '+ serverId + ' Sending sync request from update');
                        send(syncRes, successor, 'sendSyncReq');                        
                        res['result'] = Outcome.InTransit;
                    }
                    else {
                        logger.info('ServerId: '+ serverId + ' Sending response from update');
                        res = syncRes;
			flag = true;
                    }
                }
                else if (payload.failure) {
		    res['result'] = handleChainFailure(payload);
		    if(payload.failure.type == "predecessor") {
			flag = true;
		    }
		}
		else if(payload.extendChain) {
		    res['result'] = handleExtendChain(payload); 
		    if(payload.type == 2 && payload.predecessor) {
			flag = true;
		    }
		}
                else if(payload.ack) {
                    res['result'] = handleAck(payload);
                }
                else if(payload.checkLog) {
                    res = checkLogs(payload);
                    flag = true;
                }
                else if (payload.genack){
                    logger.info('ServerId: '+ serverId + ' Gen request payload: ' + fullBody);
                }
                if(flag) {
                    logger.info('ServerId: '+ serverId + ' Response: ' + JSON.stringify(res)); 
                    response.end(JSON.stringify(res));
		    flag = false;
                    if(!payload.sync && !payload.failure) {  // dont increment if sync request
                        totalSentCnt++;
                    }
                }
                else {
                    response.end();
                }
            });
        }
    }
);
server.listen(port);
logger.info('Server running at http://127.0.0.1:' + port);


/**
 * Send heart beat signals to master
 *
 * using Fiber to sleep on a thread
 */
Fiber(function() {
    var payload = {
        'heartBeat' : 1,
	'serverId' : serverId,
	'bankId' : bankId,
        'type' : serverType 
    };
    while(true) {
        // check for serverLifeTime limit IF NOT UNBOUNDED
        // before sending the heartbeat signal
        if (!serverLifeTime.UNBOUND) {
           checkMaxServiceLimit(); 
        }
        send(payload, config.master, 'sendHeartBeat');
        // sleep for delat time
        util.sleep(2000);
    }
}).run();



