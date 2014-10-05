
/* System includes */
var exec = require('child_process').exec


/* Config File includes */
var config = require('./config.json');

/* Custom includes */
var logger = require('./Logger.js');

var bankServers = [];

function parseConfig() {
    
    for(var i = 0; i < config.bank.length; i++) {
	var bankServer = {};
	bankServer['bankId'] = config.bank[i].bankId;
	bankServer['servers'] = config.bank[i].servers;
        //logger.info(JSON.stringify(bankServers));
	bankServers.push(bankServer);
    }    
}

/**
 * Function to run all the server processes
 * 
 */
function runAServer() {

    var child = exec('node ./server.js');
    
    child.stdout.on('data', function(data) {
    logger.info('stdout: ' + data);
    });

    child.stderr.on('data', function(data) {
    logger.info('stdout: ' + data);
    });

    child.on('close', function(code) {
    logger.info('closing code: ' + code);
});
}


/**
 *
 *
 *
 */
function parseServerInfo() {
    parseConfig();
    logger.info(JSON.stringify(bankServers));
    var succ = {};
    var pred = {};

    logger.info('servers list');  
    for (var i = 0; i < bankServers.length; i++){
	serversList = bankServers[i]['servers'];
	servListLen = serversList.length;
	
	logger.info(serversList);

	for (var j = 0; j < servListLen; j++) {
	var serverDict = {}	
	    serverDict['serverId'] = serversList[j].serverId;
	    if(j > 0 && j < (servListLen - 1)) {
		serverDict['successor'] = serversList[j+1]; 
		serverDict['predecessor'] = serversList[j-1];
	    }
	    else if (j == 0) {
		serverDict['successor'] = serversList[j+1]; 
	    }
	    else if (j == (servListLen - 1)) {
		serverDict['predecessor'] = serversList[j-1];
	    }
	logger.info('The successor of the server ' + servId + 'is ' + JSON.stringify(succ));
	logger.info('The predecessor of the server ' + servId + 'is ' + JSON.stringify(pred));
	
	serverDict['serverId'] = serverId;
	serverDict['successor'] = succ;
	server
	}
    }
}

parseServerInfo();

/**
 *
 *
 */
