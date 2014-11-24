/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */

var config = require('./config.json');
// var config = require('./config_headFailure.json');
// var config = require('./config_tailFailure.json');

// var reqData = require('./inconsistentHistoryPayload.json');
// var reqData = require('./samePayload.json');
// var reqData = require('./randomPayload.json');
var reqData = require('./payload.json');
//var reqData = require('./payloadMsgDrop.json');
var logger = require('./logger.js');
var util = require('./util.js');

/* System includes */
var Fiber = require('fibers');
var http = require('http');

var Outcome = {
    Processed: 0,
    InconsistentWithHistory: 1,
    InsufficientFunds: 2,
    InTransit: 3
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
var numRetries = '';
var resendDelay = '';
var resendFlag = '';
var currDelay = '';	// will hold the curr dealy for any in-transit req
var currRetriesCnt = 0;    // will hold the curr retry cnt
var checkLogFlag = -1;      // will hold the flag when the client check with tail for a req
var extendChainSleepFlag = -1;  // was used to sleep the client during extend chain
                                // not used currently

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
 * Will load the config related paramters
 */
function loadClientConfig() {
    for(var i = 0; i < config.client.length; i++) {
	if(config.client[i].clientId == clientId) {
	    port = config.client[i].port;
	    numRetries = config.client[i].numRetries;
	    resendDelay = config.client[i].resendDelay;
	    resendFlag = config.client[i].resendFlag;
	}
    }
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
    else if (opr == Operation.Transfer) {
        // logger.info('ClientId: ' + clientId  + ' Sending transfer req with reqId ' + reqId + ' on src bank: ' + bankId);
        data['transfer'] = payload;
        data['withdraw'] = 1;           // Sending to the source bank
        data['deposit'] = 0;            // Updated when src bank sends to dst bank
        dest = bankServerMap[bankId].headServer;
    }
    logger.info('ClientId: ' + clientId  + ' Performing request ' + reqId + ' on bank: ' + bankId);
    logger.info('ClientId: ' + clientId  + ' Destination info: ' + JSON.stringify(dest));
    if(payload.simFail == 1) {
        // msg NOT SENT
        // this will simulate the failure condition
        // the packet dropped on the client <--> server channel
        logger.info('ClientId: ' + clientId  + ' Simulating msg failure between Client-Server');
    }
    else {
        send(data, dest, 'client');
    }
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
            logger.info('ClientId: ' + clientId  + ' Received data: ' + str);
            if(str) {
		var resBody = JSON.parse(str);
		if(payload.query) {
		    logger.info('ClientId: ' + clientId  + ' Adding response to db');
		    responses[resBody.reqId] = resBody;
		    logger.info('ClientId: ' + clientId  + ' Responses: ' + JSON.stringify(responses));
		}
		if(payload.update) {
		    if(resBody.outcome != Outcome.InTransit) {
			logger.info('ClientId: ' + clientId  + ' Adding response to db');
			responses[resBody.reqId] = resBody;
			logger.info('ClientId: ' + clientId  + ' Responses: ' + JSON.stringify(responses));
			checkLogFlag = 1;
		    }
		}
		else if (payload.checkLog) {
		    logger.info('ClientId: ' + clientId  + ' Check Logs response received');
		    logger.info('Client: ' + clientId + " " + JSON.stringify(resBody));
		    checkLogFlag = resBody.checkLog;
		    if(resBody.checkLog == 1) {
                        if (resBody.transfer) {
                            // do nothing
                        }
                        else {
			    logger.info('ClientId: ' + clientId  + ' Adding response to db');
			    responses[resBody.reqId] = resBody;
			    logger.info('ClientId: ' + clientId  + ' Responses: ' + JSON.stringify(responses));
                        }
		    }
		}
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
var client = http.createServer(function(request, response) {
    response.writeHead(200, {'Content-Type' : 'text/plain'});
    
    logger.info('ClientId: ' + clientId  + ' Response received from Server');
    if(request.method == 'POST') {
        var str = '';
        
        request.on('data', function(chunk) {
            str += chunk;
        });

        request.on('end', function() {
            var resBody = JSON.parse(str);
            logger.info('ClientId: ' + clientId  + ' Received data: ' + str);
            
            // update the bank server map for failure msg from master
            if(resBody.failure) {
                var server = resBody.failure.server;
                if(resBody.failure.type == 'head') {
                    logger.info('ClientId: ' + clientId  + ' Updating Head server');
		    bankServerMap[resBody.failure.bankId].headServer = server;
                }
                else if(resBody.failure.type == 'tail') {
                    logger.info('ClientId: ' + clientId  + ' Updating Tail server');
                    bankServerMap[resBody.failure.bankId].tailServer = server; 
                }
		if(resendFlag) {
		    // TODO: Not Implemented right now. Been dealt via
		    // resend logic and num retries
		}
            }
	    else if(resBody.extendChain) {
                var server = resBody.extendChain.server;
		if(resBody.extendChain.flag == 0) {
		    extendChainSleepFlag = 0;   
		}
                else if (resBody.extendChain.flag == 1) {
                    logger.info('ClientId: ' + clientId  + ' Updating Tail server');
                    bankServerMap[resBody.extendChain.bankId].tailServer = server; 
		    extendChainSleepFlag = 1;   
                }
	    }
            else {
                logger.info('ClientId: ' + clientId  + ' Adding response to db');
                responses[resBody.reqId] = resBody;
                logger.info('ClientId: ' + clientId  + ' Responses: ' + JSON.stringify(responses));
            }
        });
    }
});

var arg = process.argv.splice(2);
clientId = arg[0];
loadClientConfig();	// load config parameters
client.listen(port);
logger.info('ClientId: ' + clientId  + ' Client server running at http://127.0.0.1:' + port);

// prepare the mapping for the bank
prepareBankServerMap();

/**
 * Invoking the task function 
 * to perform update operation on the server
 */
// task();

function tryResending(preReq) {
    for(;!responses[preReq.reqId];) {
	// handling the resend logic
	var currTS = new Date().getTime();
	if(currTS - currDelay > resendDelay) {
            // code commented since  client doesn't sleep during extend chain
            /*
	    if(extendChainSleepFlag == 0) {
		logger.info('ClientId: ' + clientId  + ' Client sleeping while extend chain');
		for(;extendChainSleepFlag == 0;) {
		    util.sleep(2000);
		}
		logger.info('ClientId: ' + clientId  + ' Client awake after extend chain');
	    }
            */
	    logger.info('ClientId: ' + clientId  + ' Request timed out. Resending request: ' + preReq.reqId);
	    if(currRetriesCnt < numRetries) {
		/*
		if(preReq.operation == 0) { // query opr
		    performOperation(preReq);
		    currDelay = new Date().getTime();
		    currRetriesCnt++;
		    continue;
		}
		else {
		*/
		    logger.info('ClientId: ' + clientId  + ' Checking with Tail whether operation already performed: ' + preReq.reqId);
		    // check to see if the update opr is already performed
		    var data = {
			'checkLog' : 1,
			'reqId' : preReq.reqId 
		    };
		    send(data, bankServerMap[preReq.bankId].tailServer, 'checkLog');
                             
		    for(var i = 0;checkLogFlag == -1;) {
			util.sleep(1000);
			i++;
			if(i == 5)
			    break;
		    }
                                        
		    if(checkLogFlag == 0) {
			logger.info('ClientId: ' + clientId  + ' Request not performed at the tail: ' + preReq.reqId);
			logger.info('ClientId: ' + clientId  + ' Performing request again: ' + preReq.reqId);
			checkLogFlag = -1;
			performOperation(preReq);
			currDelay = new Date().getTime();
			currRetriesCnt++;
			continue;
		    }
		    else if (checkLogFlag == 1) {
			checkLogFlag = -1;
			continue;
		    }
		//}

	    }
	    else {  // number of retries is exceeded
		logger.info('ClientId: ' + clientId  + ' Number of retries ' + currRetriesCnt + ' exceeded the limit ' + numRetries + ' Aborting request: ' + preReq.reqId);
		break;
	    }
	}
	util.sleep(2000);
    }
}

/**
 * function by which client requests for bal, withdraw or transfer 
 * to different banks.
 * bal : query requests are sent at the tail server of the bank
 * withdraw/transfer : update requests are sent at the head server of bank
 *
 * using Fiber to sleep on a thread
 */
Fiber(function() { 
    logger.info('ClientId: ' + clientId  + ' Starting to perform query/update operations');
    var data = reqData.data;
    // logger.info('Data: ' + JSON.stringify(data));
    for( var i = 0; i < data.length; i++) {
        if(data[i].clientId == clientId) {
            var payloads = data[i].payloads;
            var len = data[i].payloads.length;
            var preReq = {};
            for(var j = 0; j < len; j++) {
                var payload = payloads[j].payload;
                var reqId = payload.reqId;
                logger.info('ClientId: ' + clientId  + ' Processing request: ' + reqId);
                var nums = reqId.split(".");
                // logger.info(nums[0] + ' ' + nums[1]);
                if(nums[1] > 1) {
                    logger.info('ClientId: ' + clientId  + ' Waiting for response: ' + preReq.reqId);
                    if(responses[preReq.reqId]) {
                        performOperation(payload);
			currDelay = new Date().getTime();
			currRetriesCnt = 1;
                        preReq = payload;
                        continue;
                    }                    
                    else {
			/*
                        for(;!responses[preReq.reqId];) {
			    // handling the resend logic
			    var currTS = new Date().getTime();
			    if(currTS - currDelay > resendDelay) {
				logger.info('ClientId: ' + clientId  + ' Request timed out. Resending request: ' + preReq.reqId);
                                if(currRetriesCnt <= numRetries) {
				    if(preReq.operation == 0) { // query opr
				        performOperation(preReq);
				        currDelay = new Date().getTime();
				        currRetriesCnt++;
				        continue;
				    }
				    else {
				        logger.info('ClientId: ' + clientId  + ' Checking with Tail whether operation already performed: ' + preReq.reqId);
				        // check to see if the update opr is already performed
                                        var data = {
                                            'checkLog' : 1,
                                            'reqId' : preReq.reqId 
                                            };
                                        send(data, bankServerMap[preReq.bankId].tailServer, 'checkLog');
                                        
					for(;checkLogFlag == -1;) {
                                            util.sleep(1000);
                                        }
                                        
					if(checkLogFlag == 0) {
                                            logger.info('ClientId: ' + clientId  + ' Request not performed at the tail: ' + preReq.reqId);
					    logger.info('ClientId: ' + clientId  + ' Performing request again: ' + preReq.reqId);
				            performOperation(preReq);
				            currDelay = new Date().getTime();
				            currRetriesCnt++;
				            continue;
                                        }
                                        else if (checkLogFlag == 1) {
                                            continue;
                                        }
				    }
                                }
                                else {  // number of retries is exceeded
				    logger.info('ClientId: ' + clientId  + ' Number of retries ' + currRetriesCnt + ' exceeded the limit ' + numRetries + ' Aborting request: ' + preReq.reqId);
                                    break;
                                }
			    }
                            util.sleep(2000);
			}
			*/
			tryResending(preReq);

                        performOperation(payload);
			currDelay = new Date().getTime();
			currRetriesCnt = 1;
                        preReq = payload;
			logger.info('curr: ' + j + ' len: ' + len);
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
		    currDelay = new Date().getTime();
		    currRetriesCnt = 1;
                    preReq = payload;
                }
            }
	    tryResending(preReq);	// perform the rsend logic for the last request
        }
    }
}).run();


