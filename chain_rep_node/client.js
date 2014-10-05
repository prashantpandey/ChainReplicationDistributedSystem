/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */

var http = require('http');
var config = require('./config.json');
var reqData = require('./payload.json');
var logger = require('./Logger.js');

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

/**
 * will prepare a map of bankId vs the head and tail server
 * this structure to be used while making query/update operation
 * to bank 
 * Also in case of a failure msg from Master, this structure will be updated
 */
function prepareBankServerMap() {
    logger.info('Preparing the bank server map');
    for (var i = 0; i < config.bank.length; i++) {
        var serverDetails = {
            "headServer" : config.bank[i].headServer,
            "tailServer" : config.bank[i].tailServer
        };
        var id = config.bank[i].bankId;
        bankServerMap[id] = serverDetails;
        logger.info('Bank entry added: ' + id + ' ' + JSON.stringify(bankServerMap[id]));
    }
    logger.info('Added ' + i + ' entries to the bank details');
}

/**
 * function by which client requests for bal, withdraw or transfer 
 * to different banks.
 * bal : query requests are sent at the tail server of the bank
 * withdraw/transfer : update requests are sent at the head server of bank
 */
function task() { 
    logger.info('Starting to perform query/update operations');
    var payloads = reqData.data;
    for( var i = 0; i < payloads.length; i++) {
	var bankId = payloads[i].payload.transaction.bankId;
	var opr = payloads[i].payload.transaction.operation;
        var  data = {};
        var dest = {};
        
	if(opr == Operation.GetBalance) {
            data['query'] = payloads[i].payload;
	    dest = bankServerMap[bankId].tailServer;
	}
	else if (opr == Operation.Withdraw || opr == Operation.Deposit) {
            data['update'] = payloads[i].payload;
	    dest = bankServerMap[bankId].headServer;
	}
	
        logger.info('Performing ' + opr + ' on bank: ' + bankId);
        logger.info('Destination info: ' + JSON.stringify(dest));
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
    logger.info('payload: ' + JSON.stringify(payload));
    logger.info('dest: ' + JSON.stringify(dest));

    var options =
    {
        'host': dest.hostname,
        'port': dest.port,
        'path': '/',
        'method': 'POST',
        'headers' : { 'Content-Type' : 'application/json'
                    }
    };

    var req = http.request(options, function(response){
        var str = '';
        response.on('data', function(payload){
            str += payload;
        });
        response.on('end', function(){
            logger.info(context + ': Acknowledgement received ' + JSON.stringify(str));
        });
    });

    req.write(JSON.stringify(payload));
    req.on('error', function(e){
        logger.error(context + ': Problem occured while requesting ' + e)
    });
    req.end();
}

// prepare the mapping for the bank
prepareBankServerMap();

/**
 * Invoking the task function 
 * to perform update operation on the server
 */
task();



