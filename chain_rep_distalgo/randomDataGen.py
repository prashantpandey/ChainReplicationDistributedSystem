import sys
import random
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

    def data_generation(self):

        choice_list = [0, 1, 2]
        payloads = []
        data_dict = {}
        for i in range(0, self.nreq):
            print (i)
            flag = False
            # generate the type of request randomly
            ch = random.choice(choice_list)
            while(flag == False):
                if(ch == 0 and self.qreq < self.qmax):
                    self.qreq +=1
                    flag = True
                elif(ch == 1 and self.dreq < self.dmax):
                    self.dreq +=1
                    flag = True
                elif(ch == 2 and self.wreq < self.wmax):
                    self.wreq +=1
                    flag = True
            payload = self.gen_request(ch)
            payloads.append(payload)
        print(payloads)
        #data_dict["clientId"] = 0
        #data_dict["payloads"] = payloads

        #pprint(data_dict)
        #print(data)
        #return data_dict


    def gen_request(self, ch):
        # generate ReqId in sequence
        payloads = {}
        payload = {}
        bank_set = [100, 200, 300]
        self.reqId +=1
        payload["reqId"] = self.reqId
        payload["bankId"] = random.choice(bank_set)
        payload["accNum"] = random.randrange(1000, 2000, 100)
        payload["amount"] = random.randrange(0, 10000)
        payload["destBankId"] = 0
        payload["destAccNum"] = 0
        payload["operation"] = ch
        payloads["payload"] = payload
        return payloads


def main():

    """
    for arg in sys.argv:
        print (arg)
    """
    datagen = DataGeneartion(float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4]))
    datagen.data_generation()

main()
