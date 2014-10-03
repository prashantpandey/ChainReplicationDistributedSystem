
/*
 * Master for Banking Application.
 * It detects failure of servers and circulates info
 * about the new head/tail of a chain.
 */

/* Custom includes */
var syncMsgContext = require('./SyncMsgContext.js');
var reply = require('./Reply.js');
var request = require('./Request.js');
var logger = require('./Logger.js');
var util = require('./Util.js');

/* Config File include */
var config = require('./server_config.json');

/* System includes */
var http = require('http');
var sys = require('sys');
var fs = require('fs');
var winston = require('winston');
var und = require('underscore');

var ServerRelation = {
    Successor: 0,
    Predecessor: 1
}

var ServerType = {
    Head: 0,
    Internal: 1,
    Tail: 2
}

var serverTS = {
    serverId: "value",
    timestamp: "value"
}

function notifyAll(bankId, serverId, serverType){
    
    var clients = config.bank.clients;
    var flag = -1;

    if (serverType == 'Tail') {
	flag = 1;
    }
    else if (serverType == 'Head') {
	flag = 0;
    }

    /*
     * Update info to bankId
     */
    
    for(bankObj in config.bank) {
	if (bankObj.bankId == bankId) {
	    if(flag == 1) {
		bankObj.tailServer.serverId = serverId;
	    }
	    else if(flag == 0) {
		bankObj.headServer.serverId = serverId;
	}
    }

    /*
     * For  notifying all clients having account in bankId
     * that the bank's head/tail has changed
     */

    for(cliObj in clients) {

	/*TODO:: Change this as per client in config*/

	// pick hostname and port from the client after matching the client Id from config file
	var options = {
	    hostname : hostname,
	    port: port
	}
	
	var req = http.request(options,
	    function(response) {
		logger.info('Received ack back from client');
		});
	
	req.write("Notifying that your bankId's head/tail has changed");
    }

}

var master = http.createMaster(
    function(request, response) {
	logger.info("Master Server Started!!")
	
	//TODO:: Node handler
	
	response.writeHead(200, {'Content-Type': 'text/plain'});
        
        if(request.method == 'POST') {
            // if it is a port request then load the full body of the 
            // message
            var fullBody ='';
            request.on('data',function(chunk) {
                fullBody += chunk;
            });    
            request.on('end', function() {
                var data = JSON.parse(fullBody);
                logger.info(data);
            });
        }

        response.write('Hello World...\n from the master\n');
        response.end();
    }
);
var nodes = require("multi-node").listen({
    port: 8082,
    nodes: 4
    }, master);

//master.listen(8001);
logger.info('Master running at http://127.0.0.1:8001/')
