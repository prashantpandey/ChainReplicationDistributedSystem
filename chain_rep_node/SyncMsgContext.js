


var seqNum;
var reply;
var request; 
var serverType;

function SyncMsgContext(seqNum, req, serverType, reply) {
    this.seqNum = seqNum;
    this.request = req;
    this.serverType = serverType;
    this.reply = reply;
}

SyncMsgContext.prototype.getSeqNum = function() {
    return this.seqNum;
};


SyncMsgContext.prototype.getRequest = function() {
    return this.request;
};

SyncMsgContext.prototype.getReply = function() {
    return this.reply;
};

SyncMsgContext.prototype.getServerType = function() {
    return this.serverType;
};

module.exports = SyncMsgContext;
