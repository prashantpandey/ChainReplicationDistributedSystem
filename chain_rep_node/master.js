
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
var fs = require('fs');

/* Data Structures */
var hostname = '';
var port = '';

var bankServerMap = {};
var bankServerList = {};
var bankClientMap = {};
var serverTSMap = {};

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
 * handle server failure 
 * notify the respective servers
 * notify the respective clients
 *
 * @serverId: id of the failed server
 * @bankId: bankId to which the server belongs
 * @type: type of the failed server
 */
function handlerServerFailure(serverId, bankId, type) {

}

/**
 * Will probe the list of servers to check for the last
 * received time stamp.
 * If it is older than 5 secs then will invoke the server failure function
 */
function probeServers() {
}


/**
 * Will handle the heart beat msg from the server
 * The master will save the current time stamp against the serverId
 *
 * @payload: received payload
 */
function handleHeartBeat(payload) {
    logger.info('Master: Processing heart beat signal');
    serverTSMap[payload.serverId] = {
	'bankId' : payload.bankId,
	'timestamp' : new Date().getTime()
    };
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

/*
var nodes = require("multi-node").listen({
    port: 8082,
    nodes: 4
    }, master);
*/
