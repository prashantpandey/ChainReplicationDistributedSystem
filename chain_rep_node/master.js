
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
// var config = require('./config_headFailure.json');
// var config = require('./config_tailFailure.json');

/* System includes */
var http = require('http');
var sys = require('sys');
var Heap = require('heap');
var Fiber = require('fibers'); 

/* Data Structures */
var hostname = '';
var port = '';
var heartBeatDelay = '';
var succSeqNum = -1;
var extendChainFlag = -1;

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
	for(var j = 0; j < config.bank[i].clients.length; j++) {
	    var clientId = config.bank[i].clients[j].clientId;
	    clients[j] = {
		"hostname" : config.client[clientId].hostname,
		"port" : config.client[clientId].port
	    };     
	    // logger.info('Client List: ' + JSON.stringify(config.client[clientId]));
	}
        
	var bankId = config.bank[i].bankId;
	bankServerMap[bankId] = serverDetails;
	bankServerList[bankId] = config.bank[i].servers;
	bankClientMap[bankId] = clients;
	// logger.info('Master: Bank entry added: ' + id + ' ' + JSON.stringify(bankServerList[bankId]));
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
    if(obj) {
        // logger.info('Master: Updating old timestamp');
	obj.timestamp = new Date().getTime();
	serverTSHeap.updateItem(obj);
    }
    else {
	serverTSMap[payload.serverId] = {
	    'serverId' : payload.serverId,
	    'bankId' : payload.bankId,
	    'type' : payload.type,
	    'timestamp' : new Date().getTime()
	};
	// logger.info('Master: Adding new timestamp');
        serverTSHeap.push(serverTSMap[payload.serverId]);
    }
    // logger.info('Master: timestamp updated ' + JSON.stringify(serverTSMap[payload.serverId]));
    /*
    for(var i = 0; i < serverTSHeap.toArray().length; i++) {
	logger.info('Master: Heap ' + JSON.stringify(serverTSHeap.toArray()[i]));
    }
    */
    logger.info('Master: heart beat msg processed');
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
function handleServerFailure(serverId, bankId, type) {
    logger.info('Master: Handling the server failure for ServerId: ' + serverId);
    switch(type) {
        case 0:
            var newHead = updateChain(bankId, serverId, type);
            var payload = {
                'failure' : {
                    'type' : 'head',
                    'server' : newHead.head,
                    'bankId' : bankId
                    }
                };
	    logger.info('Master: new relation: ' + JSON.stringify(newHead));
            notifyClient(bankId, payload);
            send(payload, newHead.head, 'notifyHead'); // notify new head 
            break;
        case 1:
            var newSuccPred = updateChain(bankId, serverId, type);
            var payload = {
                'failure' : {
                    'type' : 'predecessor',
                    'server' : newSuccPred.predecessor
                    }
                };
	    logger.info('Master: new relation: ' + JSON.stringify(newSuccPred));
	    send(payload, newSuccPred.successor, 'InformSuccessor'); // notify successor
            for(;succSeqNum == -1;) {
		util.sleep(1000);
	    }
	    payload.failure['type'] = 'successor';
	    payload.failure['server'] = newSuccPred.successor;
	    payload.failure['seqNum'] = succSeqNum;
	    send(payload, newSuccPred.predecessor, 'InformPredecessor'); // notify successor
	    succSeqNum = -1;
	    break;
        case 2:
            var newTail = updateChain(bankId, serverId, type);
            var payload = {
                'failure' : {
                    'type' : 'tail',
                    'server' : newTail.tail,
                    'bankId' : bankId
                    }
                };
	    logger.info('Master: new relation: ' + JSON.stringify(newTail));
            notifyClient(bankId, payload);
            send(payload, newTail.tail, 'notifyTail'); // notify new tail 
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
            var i = 0;
	    for(i = 0; i < bankServerList[bankId].length; i++) {
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
            bankServerMap[bankId].headServer = newHead;
            return newHead;
        case 1:
            // update server list
            var i = 0;
	    for(i = 0; i < bankServerList[bankId].length; i++) {
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
            var i = 0;
	    for(i = 0; i < bankServerList[bankId].length; i++) {
                if(serverId == bankServerList[bankId][i].serverId) {
                    bankServerList[bankId].splice(i, 1);
                    break;
                }
            }
            var tailIdx = bankServerList[bankId].length - 1;
            bankServerList[bankId][tailIdx].type = 2; // i points to the new tail
            var newTail = {
                'tail' : {
                    'hostname' : bankServerList[bankId][tailIdx].hostname,
                    'port' : bankServerList[bankId][tailIdx].port
                    }
                };
            // update server map
            bankServerMap[bankId].tailServer = newTail;
            return newTail;
        default:
            logger.info('Master: Unknown server type. ServerId: ' + serverId + ' Type: ' + type);            
    }
}

/**
 * notify clients for the new head/tail server
 */
function notifyClient(bankId, payload) {
    logger.info('Master: entering notify client ' + bankClientMap[bankId].length);
    for(var i = 0; i < bankClientMap[bankId].length; i++) {
        var dest = {
            'hostname' : bankClientMap[bankId][i].hostname,
            'port' : bankClientMap[bankId][i].port
        };
	logger.info('Master: Notifying client of bankId: ' + bankId + ' dest: ' + JSON.stringify(dest));
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
            logger.info('Master: ' + context + ': Acknowledgement received' + str);
	    if(data.failure) {
		if(data.failure.type == 'predecessor') {
		    var payload = JSON.parse(str);
		    logger.info('Master: failure response: ' + JSON.stringify(payload));
		    if(payload.result.seqNum) {
			succSeqNum = payload.seqNum;
		    }
		}
	    }
	    else if(data.extendChain) {
		if(data.extendChain == 1) {
		    var payload = JSON.parse(str);
		    logger.info('Master: extendChain response: ' + JSON.stringify(payload));
		    if(payload.result.ack) {
			extendChainFlag = payload.result.ack;
		    }
		}
	    }
        });
    });

    req.write(JSON.stringify(data));
    req.on('error', function(e){
        logger.error('Master: ' + context + ': Problem occured while requesting' + e)
    });
    req.end();
}

/**
 * Add new server to the chain as the new tail
 *
 * @payload: Will have all the attributes of the new Tail server
 */
function addServer(payload) {
    var bankId = payload.bankId;
    logger.info('Master: Adding new server to the list ' + payload.serverId);
    var data = {
	'extendChain' : { 'flag' : 0 }
    };
    notifyClient( bankId, data);
    var oldTail = bankServerMap[bankId].tailServer;
    // update bank server map
    var newTail = {
	'hostname' : payload.hostname,
	'port' : payload.port
    };
    // update bank server list
    bankServerList[bankId].push(payload);
    // notifying the new server of confirmation
    // also changing the type as tail
    data = {
	'extendChain' : 1,
	'type' : 2,
	'predecessor' : oldTail
    };
    logger.info('Master: Notifying new Tail of chain extension');
    send(data, newTail, 'extendChain');	// notify new server
    for(var i = 0; extendChainFlag == -1;) {
	util.sleep(1000);
	i++;
	if(i == 5)
	    break;
    }
    if(extendChainFlag == -1) {
	logger.info('Master: Cannot extend the chain. The new server failed. Reverting back to the old chain.');
	awakeClient(bankId, OldTail);
	return;
    }
    else if(extendChainFlag == 1) {
	logger.info('Master: New tail activated successfully');
	extendChainFlag = -1;
    }
    // notify the old tail of its new successor
    logger.info('Master: New server added to the chain successfully.');
    data = {
	'extendChain' : 2,
	'type' : 1,
	'successor' : newTail
    };
    logger.info('Master: Notifying old Tail of chain extension');
    send(data, oldTail, 'extendChain');
    for(var i = 0;extendChainFlag == -1;) {
	util.sleep(1000);
	i++;
	if(i == 8)
	    break;
    }
    if(extendChainFlag == -1) {
	logger.info('Master: Cannot extend the chain. The new server failed. Reverting back to the old chain.');
	var len = bankServerList[bankId].length;
	data = {
	    'extendChain' : -1,
	    'type' : 2
	};
	logger.info('Master: Notifying  old Tail of chain extension failure');
	send(data, oldTail, 'extendChainFail');	// notify new server	
	awakeClient(bankId, OldTail);
	return;
    }
    else if(extendChainFlag == 2) {
	logger.info('Master: New tail synchronized with successfully');
	bankServerMap[bankId].tailServer = newTail;
	bankServerList[bankId].push(payload);
	// notify the clients of the new tail server
	awakeClient(bankId, newTail);
	extendChainFlag = -1;
	return;
    }
}

function awakeClient(bankId, oldTail) {
    data = {
	'extendChain' : {
	    'type' : 'tail',
	    'server' : oldTail,
	    'bankId' : bankId,
	    'flag' : 1
	}
    };
    logger.info('Master: new relation: ' + JSON.stringify(oldTail));
    notifyClient(bankId, data); 	
    logger.info('Master: Added new sever: ' + JSON.stringify(data));
}
    

/**
 * Will handle the extend chain request from a new server
 * The mater will update the server list corresponding to the chain
 * Also will inform the others servers in the chain
 *
 * @payload: received payload
 */
extendChain = Fiber(function (payload) {
    addServer(payload);
});

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

                // logger.info(data);
		// sequester the request based upon the element present
		// in the msg body
		
		if(payload.heartBeat) {
		    handleHeartBeat(payload);
		}
		else if(payload.extendChain) {
		    extendChain.run(payload.extendChain);
		}
		else if(payload.ack) {
		    logger.info('Master: extendChain response: ' + JSON.stringify(payload));
		    if(payload.ack) {
			extendChainFlag = payload.ack;
		    }
		}
		else {
		    logger.info('Master: Unknown Request Type')
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
heartBeatDelay = config.master.heartBeatDelay;
prepareBankServerMap();
master.listen(port);
logger.info('Master running at http://127.0.0.1:' + port);

/**
 * Probe the server heap struct for failure
 * pop the serverTSHeap to find if the last received timestamp
 * is expired 
 * If not sleep for 1 sec and continue
 *
 * using Fiber to sleep on a thread          
 */
Fiber(function() {
    while(true) {
        logger.info('Master: probing the server heap for failure');
        var currTS = new Date().getTime();
        var server = serverTSHeap.peek();
        if(server && ((currTS - server.timestamp) > 5000)) { // server has failed
            logger.info('Master: ServerId: ' + server.serverId + ' failed');
	    logger.info('Master: ' + currTS + ' ' + server.timestamp);
            server = serverTSHeap.pop();
            handleServerFailure(server.serverId, server.bankId, server.type);
        }
        // else sleep for a sec
        util.sleep(1000);
    }
}).run();
