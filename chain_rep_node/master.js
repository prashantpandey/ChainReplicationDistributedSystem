
/*
 * Master for Banking Application.
 * It detects failure of servers and circulates info
 * about the new head/tail of a chain.
 * Also supports extend chain functionality.
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
var Heap = require('heap');

/* Data Structures */
var hostname = '';
var port = '';

var bankServerMap = {};
var bankServerList = {};
var bankClientMap = {};

/**
 * dictionary for keeping the serverId vs timestamp map
 * this structure will be overlaied on the heap to update the
 * values in the heap.
 */
var serverTSMap = {};

/** 
 * create a min-heap implementation for storing 
 * the last received timestamp (lrt) for each server
 * across the chain.
 *
 * It has a custom comparison function for comparing the 
 * elements based upon their timestamp
 * */
var serverTSHeap = new Heap(function(a, b) {
    return a.timestamp - b.timestamp;
});

/* General functions */

/**
 * will prepare 
 * 1. a map of bankId vs the head and tail server
 * 2. a map of bankId vs the server List in that chain
 * 3. a map of bankId vs the clients attached to that bank
 * this structure to be used to handle the extend Chain or server failure.
 * to bank 
 *
 * Also in case of a failure, this structure will be updated
 */
function prepareBankServerMap() {
    logger.info('Master:  Preparing the bank server map');
    for (var i = 0; i < config.bank.length; i++) {
        var serverDetails = {
            "headServer" : config.bank[i].headServer,
            "tailServer" : config.bank[i].tailServer
        };
	var clients = [];
	for(var j = 0; j < config.bank[i].clients; j++) {
	    var clientId = config.bank[i].clients[j].clientId;
	    client[j] = {
		"hostname" : config.client[clientId].hostname;
		"port" : config.client[clientId].port;
	    };     
	}
        
	var bankId = config.bank[i].bankId;
	bankServerMap[bankId] = serverDetails;
	bankServerList[bankId] = config.bank[i].servers;
	bankClientMap[bankId] = clients;
        // logger.info('Master: Bank entry added: ' + id + ' ' + JSON.stringify(bankServerMap[id]));
    }
    // logger.info('Master: Added ' + i + ' entries to the bank details');
}

/**
 * Will handle the heart beat msg from the server
 * The master will save the current time stamp against the serverId
 *
 * @payload: received payload
 */
function handleHeartBeat(payload) {
    logger.info('Master: Processing heart beat signal from Server: ' +  payload.serverId);

    // check if their is already a timestamp from the server
    var obj = serverTSMap[payload.serverId];
    serverTSMap[payload.serverId] = {
        'serverId' : payload.serverId,
	'bankId' : payload.bankId,
	'timestamp' : new Date().getTime()
    };
    if(obj)
        serverTSHeap.update(obj);
    else 
        serverTSHeap.push(obj);
    logger.info('Master: heart beat msg processed');
}

/**
 * Probe the server heap struct for failure
 * pop the serverTSHeap to find if the last received timestamp
 * is expired 
 */
function probeServerHeap() {
    logger.info('Master: probing the server heap for failure');
    var curTS = new Date().getTime();
    var server = serverTSHeap.peek();
    if(curTS - server.timestamp > 5) { // server has failed
        logger.info('Master: ServerId: ' + server.serverId + ' failed');
        handleServerFailure(server.serverId, server.bankId, server.type);
    }
    // else do nothing 
}

/**
 * handle server failure 
 * notify the respective servers
 * notify the respective clients
 *
 * @serverId: id of the failed server
 * @bankId: bankId to which the server belongs
 * @type_: type of the failed server
 */
function handlerServerFailure(serverId, bankId, type) {
    logger.info('Master: Handling the server failure for ServerId: ' + serverId);
    switch(type) {
        case 0:
            var newHead = updateChain(bankId, serverId, type);
            var payload = {
                'type' : 'head',
                'server' : newHead
                };
            notifyClient(bankId, payload);
            send(payload, newHead, 'notifyHead'); // notify new head 
            break;
        case 1:
            var newSuccPred = updateChain(bankId, serverId, type);
            new payload = {
                'type' : 'predecessor',
                'server' : newSuccPred.successor
                };
            notifyServer(newSuccPred.predecessor, payload); // notify pred
            payload['server'] = newSuccPred.predecessor;
            notifyServer(newSuccPred.successor, payload); // notify succ
            break;
        case 2:
            var newTail = updateChain(bankId, serverId, type);
            var payload = {
                'type' : 'tail',
                'server' : newTail
                };
            notifyClient(bankId, payload);
            send(payload, newHead, 'notifyTail'); // notify new tail 
            break;
        default:
            logger.info('Master: Unknown server type. ServerId: ' + serverId + ' Type: ' + type);
    }
}

/**
 * update the server chain (local data structures) to reflect server failure
 */
function updateChain(bankId, serverId, type) {
    var response = {};
    switch(type) {
        case 0:
            // update server list
            for(var i = 0; i < bankServerList[bankId].length; i++) {
                if(serverId == bankServerList[bankId][i].serverId) {
                    bankServerList[bankId].splice(i, 1);
                    break;
                }
            }
            // since the array reindexes itself after splice
            bankServerList[bankId][i].type = 0; // i points to the new head
            var newHead = {
                'head' : {
                    'hostname' : bankServerList[bankId][i].hostname,
                    'port' : bankServerList[bankId][i].port
                    }
                };
            // update server map
            bankServerMap[bankId].serverDetails.headServer = newHead;
            return newHead;
        case 1:
            // update server list
            for(var i = 0; i < bankServerList[bankId].length; i++) {
                if(serverId == bankServerList[bankId][i].serverId) {
                    bankServerList[bankId].splice(i, 1);
                    break;
                }
            }
            var newServers = {
                'predecessor' : {
                    'hostname' : bankServerList[bankId][i-1].hostname,
                    'port' : bankServerList[bankId][i-1].port
                    },
                'successor' : {
                    'hostname' : bankServerList[bankId][i].hostname,
                    'port' : bankServerList[bankId][i].port
                    }
                };
            return newServers;
        case 2:
            // update server list
            for(var i = 0; i < bankServerList[bankId].length; i++) {
                if(serverId == bankServerList[bankId][i].serverId) {
                    bankServerList[bankId].splice(i, 1);
                    break;
                }
            }
            var tailIdx = bankServerList[bankId].length;
            bankServerList[bankId][tailIdx].type = 2; // i points to the new tail
            var newTail = {
                'tail' : {
                    'hostname' : bankServerList[bankId][tailIdx].hostname,
                    'port' : bankServerList[bankId][tailIdx].port
                    }
                };
            // update server map
            bankServerMap[bankId].serverDetails.headServer = newTail;
            return newTail;
        default:
            logger.info('Master: Unknown server type. ServerId: ' + serverId + ' Type: ' + type);            
    }
}

/**
 * notify clients for the new head/tail server
 */
function notifyClient(bankId, payload) {
    for(var i = 0; i < bankClientMap[bankId].length; i++) {
        var dest = {
            'hostname' : bankClientMap[bankId][i].hostname,
            'port' : bankClientMap[bankId][i].port
        };
        send(payload, dest, 'notifyClient');
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
    logger.info('Master: ' + JSON.stringify(dest) + ' ' + context);
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
 * Will handle the extend chain request from a new server
 * The mater will update the server list corresponding to the chain
 * Also will inform the others servers in the chain
 *
 * @payload: received payload
 */
function extendChain(payload) {

}

var master = http.createServer(
    function(request, response) {
	// logger.info("Master: Started!!")
	response.writeHead(200, {'Content-Type': 'text/plain'});
	
	// call request handler
	// this function will handle all the events
	// 1. receive the heart-beat from servers
	// 2. receive the extendChain from new server

        if(request.method == 'POST') {
            var fullBody ='';
	    var res = {};
	    
	    // if it is a POST request then load the full msg body
            request.on('data',function(chunk) {
		// logger.info('got data');
                fullBody += chunk;
            });

            request.on('end', function() {
		// logger.info('got end');

		// parse the msg body to JSON once it is full received
                var payload = JSON.parse(fullBody);

                logger.info(data);
		// sequester the request based upon the element present
		// in the msg body
		
		if(payload.heartbeat) {
		    receiveHeartBeat(payload);
		}
		else if(payload.extendChain) {
		}
		else {
		    logger.info('Unknown Request Type')
		    res['result'] = "";
		}
		
		// no specific response needed other than ack
		// from the master
		response.end();
            });
        }
    }
);

// read the port name from the config file
hostname = config.master.hostname;
port = config.master.port;
master.listen(port);
logger.info('Master running at http://127.0.0.1:' + port);

