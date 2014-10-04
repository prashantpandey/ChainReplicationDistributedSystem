/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */

var http = require('http');
var config = require('./config.json');
var data = require('./payload.json');

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


var bankServerMap = [];

/**
 * will prepare a map of bankId vs the head and tail server
 * this structure to be used while making query/update operation
 * to bank 
 * Also in case of a failure msg from Master, this structure will be updated
 */
function prepareBankServerMap() {
    for (var i = 0; i < config.bank.length; i++) {
        var serverDetails = {
            "headServer" : config.bank[i].headServer,
            "tailServer" : config.bank[i].tailServer
        };
        var id = config.bank[i].bankId;
        bankServerMap = { id : serverDetails };
    }
}

/**
 * function by which client requests for bal, withdraw or transfer 
 * to different banks.
 * bal : query requests are sent at the tail server of the bank
 * withdraw/transfer : update requests are sent at the head server of bank
 */
function task() {
    // prepare the mapping for the bank
    prepareBankServerMap();

    for( var i = 0; i < data.length; i++) {
	var bankId = data[i].transaction.bankId;
	var opr = data[i].transaction.operation;

	if(opr == Operation.GetBalance) {
	    dest = bankServerMap[bankId].tailServer;
	}
	else if (opr == Operation.Withdraw || opr == Operation.Deposit) {
	    dest = bankServerMap[bankId].headServer;
	}
	
        var sendData = data[i].payload;
	send(sendData, dest, 'client');   
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
    var options =
    {
        'host': dest.hostname,
        'port': dest.port,
        'path': '/',
        'method': 'POST',
        'headers' : { 'Content-Type' : 'application/json',
                       'Content-Length' : 'chunked'
                    }
    };

    var req = http.request(options, function(){
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
 * Invoking the task function 
 * tp perform update operation on the server
 */
task();
