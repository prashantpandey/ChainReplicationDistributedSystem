__author__ = 'kavita'
import sys
import random
import json
from pprint import pprint
class DataGeneartion:
    def __init__(self, pquery, pdeposit, pwithdraw, nreq):
        self.pdeposit = pdeposit
        self.pquery = pquery
        self.pwithdraw = pwithdraw
        self.nreq = nreq
        self.qreq = 0
        self.qmax = self.pquery * self.nreq
        self.dreq = 0
        self.dmax = self.pdeposit * self.nreq
        self.wreq = 0
        self.wmax = self.pwithdraw * self.nreq
        self.reqId = 0;

    def data_generation(self, clientId):
        choice_list = [0, 1, 2]
        payloads = []
        data_dict = {}
        for i in range(0, self.nreq):
            # generate the type of request randomly
            ch = random.choice(choice_list)

            if(ch == 0 and self.qreq < self.qmax):
                self.qreq +=1
            elif(ch == 1 and self.dreq < self.dmax):
                self.dreq +=1
            elif(ch == 2 and self.wreq < self.wmax):
                self.wreq +=1

            payload = self.gen_request(ch, clientId)
            payloads.append(payload)
        data_dict["payloads"] = payloads
        data_dict["clientId"] = clientId
        #pprint(data_dict)
        return data_dict


    def gen_request(self, ch, clientId):
        # generate ReqId in sequence
        payloads = {}
        payload = {}
        bank_set = [100, 200, 300]
        self.reqId +=1
        payload["reqId"] = str(clientId) + "." + str(self.reqId)
        payload["bankId"] = random.choice(bank_set)
        payload["accNum"] = random.randrange(1000, 2000, 100)
        payload["amount"] = random.randrange(0, 10000)
        payload["destBankId"] = 0
        payload["destAccNum"] = 0
        payload["hostname"] = "localhost"
        payload["port"] = 8111 + int(clientId)
        payload["operation"] = ch
        payloads["payload"] = payload
        return payloads


def main():
    data_list = []
    final_dict = {}
    datagen = DataGeneartion(float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4]))
    for i in range(0,6):
        data_dict = {}
        data_dict = datagen.data_generation(i)
        datagen.reqId = 0
        data_list.append(data_dict)
    final_dict["data"] = data_list

    with open('randomPayload.json', 'w') as f:
        json.dump(final_dict, f, indent=4, ensure_ascii=False)

main()
