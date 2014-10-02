

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

Request.protype.getBankId = fuction() {
    return this.bankId;
}

Request.protype.getAccNum = fuction() {
    return this.accNum;
}

Request.protype.getAmount = fuction() {
    return this.amount;
}

Request.protype.getDestBankId = fuction() {
    return this.destbankId;
}

Request.protype.getDestAccNum = fuction() {
    return this.destAccNum;
}

module.exports = Request;
