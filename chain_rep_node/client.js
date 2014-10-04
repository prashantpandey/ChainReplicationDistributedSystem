
/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */


var http = require('http');
var config = require('./config.json');
var data = require('./payload.json');
/**
 * function by which client requests for bal, withdraw or transfer 
 * to different banks.
 * bal : query requests are sent at the tail server of the bank
 * withdraw/transfer : update requests are sent at the head server of bank
 */

function task() {    
    for( var i = 0; i < data.length; i++) {
	var bankId = data[i].transaction.bankId;
	var opr = data[i].transaction.operation;
	for(var j = 0; j < config.bank.length; i++) {
	    if(bankId == config.bank[j].bankId) {
		if(opr == 'bal') {
		    dest = config.bank[j].tailServer;
		}
		else if (opr == 'withdraw' || opr == 'deposit') {
		    dest = config.bank[j].headServer;
		}
	    }
	    var sendData = data[i].payload;
	    send(sendData, dest, 'client');   
	}
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

task();
