/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */

var http = require('http');
var config = require('./config.json');
var reqData = require('./payload.json');
var logger = require('./logger.js');

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
    logger.info('Preparing the bank server map');
    for (var i = 0; i < config.bank.length; i++) {
        var serverDetails = {
            "headServer" : config.bank[i].headServer,
            "tailServer" : config.bank[i].tailServer
        };
        var id = config.bank[i].bankId;
        bankServerMap[id] = serverDetails;
        // logger.info('Bank entry added: ' + id + ' ' + JSON.stringify(bankServerMap[id]));
    }
    // logger.info('Added ' + i + ' entries to the bank details');
}

/**
 * function by which client requests for bal, withdraw or transfer 
 * to different banks.
 * bal : query requests are sent at the tail server of the bank
 * withdraw/transfer : update requests are sent at the head server of bank
 */
function task() { 
    logger.info('Starting to perform query/update operations');
    var data = reqData.data;
    // logger.info('Data: ' + JSON.stringify(data));
    for( var i = 0; i < data.length; i++) {
        if(data[i].clientId == clientId) {
            var payloads = data[i].payloads;
            var len = data[i].payloads.length;
            for(var j = 0; j < len; j++) {
                var payload = payloads[j].payload;
	        var bankId = payload.bankId;
        	var opr = payload.operation;
                var  data = {};
                var dest = {};
        
	        if(opr == Operation.GetBalance) {
                    data['query'] = payload;
        	    dest = bankServerMap[bankId].tailServer;
	        }
        	else if (opr == Operation.Withdraw || opr == Operation.Deposit) {
                    data['update'] = payload;
        	    dest = bankServerMap[bankId].headServer;
	        }
	
                logger.info('Performing ' + opr + ' on bank: ' + bankId);
                logger.info('Destination info: ' + JSON.stringify(dest));
        	send(data, dest, 'client');   
            }
        }
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
            if(payload.query) {
                logger.info('Adding response to db');
                responses[str.reqId] = str;
            }
        });
    });

    req.write(JSON.stringify(payload));
    req.on('error', function(e){
        logger.error(context + ': Problem occured while requesting ' + e)
    });
    req.end();
}

/**
 * Server to receive responses from the tail
 *
 */
var server = http.createServer(function(request, response) {
    response.writeHead(200, {'Content-Type' : 'text/plain'});
    
    logger.info('Response received from Server');
    if(request.method == 'POST') {
        var str = {};
        
        response.on('data', function(chunk) {
            str += chunk;
        });

        request.on('end', function() {
            logger.info('Adding response to db');
            responses[str.reqId] = str;
        });
    }

});

var arg = process.argv.splice(2);
clientId = arg[0];
port = arg[1];
server.listen(port);
logger.info('Client server running at http://127.0.0.1:' + port);

// prepare the mapping for the bank
prepareBankServerMap();

/**
 * Invoking the task function 
 * to perform update operation on the server
 */
task();



