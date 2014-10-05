
/* System includes */
var exec = require('child_process').exec

/* Config File includes */
var config = require('./config.json');

/* Custom includes */
var logger = require('./Logger.js');

/* Global declarations */
var bankServers = [];


/**
 * common util function to parse the bank and its servers from the config file
 * This gives a bankServers map which is a global Map
 */
function parseConfig() {
    logger.info('Parsing the config file to get all the bank and respective servers.');    
    for(var i = 0; i < config.bank.length; i++) {
	var bankServer = {};
	bankServer['bankId'] = config.bank[i].bankId;
	bankServer['servers'] = config.bank[i].servers;
        //logger.info(JSON.stringify(bankServers));
	bankServers.push(bankServer);
	logger.info('Added ' + (i+1) + ' th bank and its servers in bankServers map.');
    }
}

/**
 * Function to run all the server processes
 * This is called from runAllServers for each bank's server
 * @serverDict : A map with all the server's info 
 */
function runAServer(serverDict) {

    var cmdArg = JSON.stringify(serverDict);
    logger.info(cmdArg);

    var child = exec('node ./server.js');

    if (child)
	logger.info('Server with id :'+ serverDict['serverId'] + 'execed.');
    
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
 * parses the bankServers map to start the server for each bank
 * returns a serverDict which goes as a cmd arg to the runAServer function
 */
function parseServerInfo() {
    parseConfig();
//    logger.info(JSON.stringify(bankServers));
    var succ = {};
    var pred = {};

//    logger.info('servers list');  
    for (var i = 0; i < bankServers.length; i++){
	serversList = bankServers[i]['servers'];
	servListLen = serversList.length;
	
	var bankId = bankServers[i]['bankId'];
//	logger.info(bankId);

	for (var j = 0; j < servListLen; j++) {
	    var serverDict = {}	
	    var serverId = serversList[j].serverId;
	    serverDict['serverId'] = serverId;
	    serverDict['bankId'] = bankId;
	    serverDict['hostname'] = serversList[j].hostname;
	    serverDict['port'] = serversList[j].port;
	    serverDict['startupDelay'] = serversList[j].startupDelay;
	    serverDict['serverLifeTime'] = serversList[j].serverLifeTime;
	    serverDict['successor'] = findSuccPredServer(bankId, serverId, 1); 
	    serverDict['predecessor'] = findSuccPredServer(bankId, serverId, 0);
	    logger.info('Attempting to run server for bankId:' + bankId + ' serverId: '+ serverId);
	runAServer(serverDict);	
	}
    }
    return;
}

/**
 * generic function to find the successor and predecessor of a bank's
 * server.
 * @bankId : bank Id
 * @serverId : server Id for which succ/pred has to be found
 * succFlag : 1 means successor has to be found, 0 means predecessor
 */
function findSuccPredServer(bankId, serverId, succFlag) {
    var serverList;
    var servListLen;

    if (succFlag)
	logger.info('Computing successor of serverId :' + serverId + 'for bankId:' + bankId );
    else
	logger.info('Computing predecessor of serverId :' + serverId + 'for bankId:' + bankId );
    
    for(var i = 0; i < bankServers.length; i++) {
	if(bankServers[i]['bankId'] == bankId) {
	    serverList = bankServers[i]['servers'];
	    servListLen = serversList.length;
	}
    }
    
    for (var j = 0; j < servListLen; j++) {
	if(serverList[j].serverId == serverId) {	    
	    if((j < (servListLen - 1)) && succFlag)
		return serversList[j+1];
	    else if((j == servListLen - 1) && succFlag) 
		return {};
	    else if((j > 0) && !succFlag)
		return serversList[j-1];
	    else if((j == 0) && !succFlag)
		return {}; 
	    break;
	}
    }
    logger.info('Exiting predecessors and successor functions');
}

parseServerInfo();

/**
 *
 *
 */
