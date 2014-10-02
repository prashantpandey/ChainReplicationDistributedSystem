
var reqId;
var outcome;
var bal;

function Reply(reqId, outcome, bal) {
    this.reqId = reqId;
    this.outcome = outcome;
    this.bal = bal;
}

Reply.prototype.getReqId = function() {
    return this.reqId;
}

Reply.prototype.getOutcome = function() {
    return this.outcome;
}

Reply.prototype.getBal = function() {
    return this.bal;
}

module.exports = Reply;

