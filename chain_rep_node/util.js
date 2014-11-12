
/* System includes */
var exec = require('child_process').exec
var Fiber = require('fibers');

/* Config File includes */
// var config = require('./config.json');
var config = require('./config_extendChain.json');
// var config = require('./config_headFailure.json');
// var config = require('./config_tailFailure.json');

/* Custom includes */
var logger = require('./logger.js');

/* Global declarations */
var bankServers = [];


/**
 * common util function to parse the bank and its servers from the config file
 * This gives a bankServers map which is a global Map
 */
function parseConfig() {
    //logger.info('Parsing the config file to get all the bank and respective servers.');    
    for(var i = 0; i < config.bank.length; i++) {
	var bankServer = {};
	bankServer['bankId'] = config.bank[i].bankId;
	bankServer['servers'] = config.bank[i].servers;
        //logger.info(JSON.stringify(bankServers));
	bankServers.push(bankServer);
	//logger.info('Added ' + (i+1) + ' th bank and its servers in bankServers map.');
    }
}

/**
 * parses the bankServers map to start the server for each bank
 * returns a serverDict which goes as a cmd arg to the runAServer function
 */

exports.parseServerInfo = function parseServerInfo(bankId, serverId) {
    parseConfig();
    // logger.info('In parse server info function. bankId: ' + bankId + ' serverId: ' + serverId);
    // logger.info(JSON.stringify(bankServers));
    var succ = {};
    var pred = {};
    var serverDict = {};
    var flag = 0;
    for (var i = 0; i < bankServers.length; i++) {
	if (bankServers[i]['bankId'] == bankId) {
	    serversList = bankServers[i]['servers'];
	    servListLen = serversList.length;
	    
	    for (var j = 0; j < servListLen; j++) {
		if(serversList[j].serverId == serverId) {
		    serverDict['hostname'] = serversList[j].hostname;
		    serverDict['port'] = serversList[j].port;
		    serverDict['type'] = serversList[j].type;
		    serverDict['startupDelay'] = serversList[j].startupDelay;
		    serverDict['serverLifeTime'] = serversList[j].serverLifeTime;
		    serverDict['successor'] = findSuccPredServer(bankId, serverId, 1); 
		    serverDict['predecessor'] = findSuccPredServer(bankId, serverId, 0);
		    serverDict['fail'] = serversList[j].fail;
		    flag = 1;
		    break;	    
		}
	    }
	    if(flag)
		break;
	}
    }
    if(flag) {
	// logger.info(JSON.stringify(serverDict));
	return serverDict;
    }
}

/**
 * generic function to find the successor and predecessor of a bank's
 * server.
 *
 * @bankId : bank Id
 * @serverId : server Id for which succ/pred has to be found
 * succFlag : 1 means successor has to be found, 0 means predecessor
 */
function findSuccPredServer(bankId, serverId, succFlag) {
    parseConfig();

    // logger.info('In sucessor and predecessor function');
    var serverList;
    var servListLen;

    /*
    if (succFlag)
        logger.info('Computing successor of serverId :' + serverId + 'for bankId:' + bankId );
    else
        logger.info('Computing predecessor of serverId :' + serverId + 'for bankId:' + bankId );
   */

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
    // logger.info('Exiting predecessors and successor functions');
}

//parseServerInfo(100, "104");

/**
 *    
 */
exports.sleep = function sleep(ms) {
    var fiber = Fiber.current;
    setTimeout(function() {
	fiber.run();
    }, ms);
    Fiber.yield();
}

