
/*
 * Client side for Banking Application.
 * The server side implements Chain Replication Algo.
 * The client communicates with server for banking operations.
 */


var http = require('http');
var und = require('underscore');

var reqList = [
    {
    'reqId': 
    'status':
    },
    {
    'reqId':
    'status':
    } 
]
/**
 * function by which client requests for bal, withdraw or transfer 
 * to different banks.
 * bal : query requests are sent at the tail server of the bank
 * withdraw/transfer : update requests are sent at the head server of bank
 */

function task() {    
    for(obj in data.payload) {
	var bankId = obj.transaction.bankId;
	var opr = obj.transaction.operation;
	for(bankObj in config.bank) {
	    if(bankId == bankObj.bankId) {
		if(opr == 'bal') {
		    dest = bankObj.tailServer;
		}
		else if (opr == 'withdraw' || opr == 'deposit') {
		    dest = bankObj.headServer;
		}
	    }
	    var sendData = data.payload;
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
