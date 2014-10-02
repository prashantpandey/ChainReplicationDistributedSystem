

var reqId;
var bankId;
var accNum;
var amount;
var destBankId;
var destAccNum;

function Request(reqId, bankId, accNum, amount, destBankId, destAccNum) {
    this.reqId = reqId;
    this.bankId = bankId;
    this.accNum = accNum;
    this.amount = amount;
    this.destBankId = destBankId;
}

Request.prototype.getReqId = function() {
    return this.reqId;
}

Request.prototype.getBankId = function() {
    return this.bankId;
}

Request.prototype.getAccNum = function() {
    return this.accNum;
}

Request.prototype.getAmount = function() {
    return this.amount;
}

Request.prototype.getDestBankId = function() {
    return this.destbankId;
}

Request.prototype.getDestAccNum = function() {
    return this.destAccNum;
}

module.exports = Request;
