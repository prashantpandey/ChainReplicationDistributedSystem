var und = require('underscore');

function findSuccessor(bankId, serverId) {
    
    var i;
    for(bankObj in config.bank) {
	if(bankObj.bankId == bankId) {
	    servlist = bankObj.bankId.servers;
	    servIdList = und.map(servList, und.iteratee('serverId'));
	    i = indexOf(servIdList, serverId);
	    break;
	}
    }
    if(i < (length-1))
	return servList[i+1];
    return NULL; 

    
}

function findPredecessor(bankId, serverId) {
    
    var i;
    for(bankObj in config.bank) {
	if(bankObj.bankId == bankId) {
	    servlist = bankObj.bankId.servers;
	    servIdList = und.map(servList, und.iteratee('serverId'));
	    i = indexOf(servIdList, serverId);
	    break;
	}
    }
    if(i >0 && i < length)
	return servList[i-1];
    return NULL;

}
