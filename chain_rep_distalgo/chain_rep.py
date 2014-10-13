
import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('Query'), da.pat.FreePattern('req')])
PatternExpr_1 = da.pat.FreePattern('p')
PatternExpr_2 = da.pat.TuplePattern([da.pat.ConstantPattern('Update'), da.pat.FreePattern('req')])
PatternExpr_3 = da.pat.FreePattern('p')
PatternExpr_4 = da.pat.TuplePattern([da.pat.ConstantPattern('Sync'), da.pat.FreePattern('req')])
PatternExpr_5 = da.pat.FreePattern('p')
PatternExpr_6 = da.pat.TuplePattern([da.pat.ConstantPattern('Ack'), da.pat.FreePattern('reqId'), da.pat.FreePattern('serverId')])
PatternExpr_7 = da.pat.FreePattern('p')
PatternExpr_8 = da.pat.TuplePattern([da.pat.ConstantPattern('Response'), da.pat.FreePattern('res')])
import sys
import json
import pickle
import re
FLAGS = ((re.VERBOSE | re.MULTILINE) | re.DOTALL)
WHITESPACE = re.compile('[ \\t\\n\\r]*', FLAGS)

class ConcatJSONDecoder(json.JSONDecoder):

    def decode(self, s, _w=WHITESPACE.match):
        s_len = len(s)
        objs = []
        end = 0
        while (not (end == s_len)):
            (obj, end) = self.raw_decode(s, idx=_w(s, end).end())
            end = _w(s, end).end()
            objs.append(obj)
        return objs

class Server(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_0', PatternExpr_0, sources=[PatternExpr_1], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_0]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_1', PatternExpr_2, sources=[PatternExpr_3], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_1]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_2', PatternExpr_4, sources=[PatternExpr_5], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_2]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_3', PatternExpr_6, sources=[PatternExpr_7], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_3])])

    def main(self):
        self.output((('ServerId: ' + str(self.serverId)) + ' starting the server operations'))
        _st_label_27 = 0
        while (_st_label_27 == 0):
            _st_label_27 += 1
            if False:
                _st_label_27 += 1
            else:
                super()._label('_st_label_27', block=True)
                _st_label_27 -= 1

    def setup(self, clients, config, pred, succ):
        self.config = config
        self.succ = succ
        self.clients = clients
        self.pred = pred
        self.serverId = config['serverId']
        self.accDetails = {}
        self.history = {}
        self.sentReq = {}
        self.clientProcessList = list(clients)

    def _Server_handler_0(self, req, p):
        self.output(((('ServerId: ' + str(self.serverId)) + ' ') + json.dumps(req)))
        self.output(((((('ServerId: ' + str(self.serverId)) + ' Received Query request: ') + str(req['reqId'])) + ' from client: ') + str(req['clientId'])))
        num = req['accNum']
        res = {}
        res['reqId'] = req['reqId']
        res['outcome'] = 'Processed'
        res['accNum'] = num
        if (num in self.accDetails):
            res['currBal'] = self.accDetails[num]
        else:
            self.output('Account does not exists. Creating new account')
            self.accDetails[num] = 0
            res['currBal'] = 0
        self._send(('Response', res), p)
    _Server_handler_0._labels = None
    _Server_handler_0._notlabels = None

    def _Server_handler_1(self, req, p):
        self.output(((('ServerId: ' + str(self.serverId)) + ' ') + json.dumps(req)))
        self.output(((((('ServerId: ' + str(self.serverId)) + ' Received Update request: ') + str(req['reqId'])) + ' from client: ') + str(req['clientId'])))
        num = req['accNum']
        amt = req['amount']
        reqId = req['reqId']
        res = {}
        res['reqId'] = reqId
        res['accNum'] = num
        if (num in self.accDetails):
            bal = self.accDetails[num]
            if (req['operation'] == 1):
                self.accDetails[num] = (bal + amt)
                self.output(((('ServerId: ' + str(self.serverId)) + ' Updating the bal: ') + str((bal + num))))
                res['outcome'] = 'Processed'
            elif (req['operation'] == 2):
                if (bal < amt):
                    self.output((('ServerId: ' + str(self.serverId)) + ' Not sufficient balance'))
                    res['outcome'] = 'InsufficientBalance'
                else:
                    self.accDetails[num] = (bal - amt)
                    res['outcome'] = 'Processed'
        else:
            self.output((('ServerId: ' + str(self.serverId)) + ' Account does not exists. Creating new account'))
            if (req['operation'] == 1):
                self.accDetails[num] = amt
            else:
                self.accDetails[num] = 0
            res['outcome'] = 'Processed'
        res['currBal'] = self.accDetails[num]
        res['payload'] = req
        self.history[reqId] = req
        self.sentReq[reqId] = req
        self._send(('Sync', res), self.succ)
    _Server_handler_1._labels = None
    _Server_handler_1._notlabels = None

    def _Server_handler_2(self, req, p):
        self.output(((('ServerId: ' + str(self.serverId)) + ' ') + json.dumps(req)))
        self.output(((('ServerId: ' + str(self.serverId)) + ' Received Sync request: ') + str(req['reqId'])))
        num = req['payload']['accNum']
        reqId = req['reqId']
        clientId = req['payload']['clientId']
        self.accDetails[num] = req['currBal']
        self.history[reqId] = req['payload']
        self.sentReq[reqId] = req['payload']
        if (self.config['type'] == 2):
            client = self.clientProcessList[clientId]
            del req['payload']
            res = req
            self._send(('Response', res), client)
            self._send(('Ack', reqId, self.serverId), self.pred)
        else:
            self._send(('Sync', req), self.succ)
    _Server_handler_2._labels = None
    _Server_handler_2._notlabels = None

    def _Server_handler_3(self, reqId, p, serverId):
        self.output(((('ServerId: ' + str(serverId)) + ' ') + str(reqId)))
        self.output(((('ServerId: ' + str(serverId)) + ' Received Ack request: ') + str(reqId)))
        nums = reqId.split('.')
        for i in range(0, int(nums[1])):
            key = ((nums[0] + '.') + str(i))
            if (key in self.sentReq):
                del self.sentReq[key]
    _Server_handler_3._labels = None
    _Server_handler_3._notlabels = None

class Client(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_0', PatternExpr_8, sources=None, destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_4])])

    def main(self):
        self.output((('ClientId: ' + str(self.clientId)) + ' starting the client operations'))
        for d in self.data:
            for item in d['data']:
                if (self.clientId == item['clientId']):
                    for payload in item['payloads']:
                        req = payload['payload']
                        req['clientId'] = self.clientId
                        reqId = req['reqId']
                        type = ''
                        if (req['operation'] == 0):
                            type = 'Query'
                        else:
                            type = 'Update'
                        clk = self.logical_clock()
                        nums = reqId.split('.')
                        if (int(nums[1]) > 1):
                            self.output(((('ReqId: ' + reqId) + ' LastReqId: ') + self.lastRecv))
                            _st_label_129 = 0
                            while (_st_label_129 == 0):
                                _st_label_129 += 1
                                if (int(self.numsLR[1]) == (int(nums[1]) - 1)):
                                    _st_label_129 += 1
                                else:
                                    super()._label('_st_label_129', block=True)
                                    _st_label_129 -= 1
                            else:
                                if (_st_label_129 != 2):
                                    continue
                            if (_st_label_129 != 2):
                                break
                        self.output(((('ClientId: ' + str(self.clientId)) + ' Sending request to server: ') + str(req['reqId'])))
                        idx = self.findServer(req['operation'], req['bankId'])
                        p = self.serverProcessList[idx]
                        self._send((type, req), p)
        _st_label_134 = 0
        while (_st_label_134 == 0):
            _st_label_134 += 1
            if False:
                _st_label_134 += 1
            else:
                super()._label('_st_label_134', block=True)
                _st_label_134 -= 1

    def setup(self, servers, config, data):
        self.config = config
        self.data = data
        self.servers = servers
        self.clientId = config['clientId']
        self.serverProcessList = list(servers)
        self.lastRecv = '0.0'
        self.numsLR = self.lastRecv.split('.')
        self.responses = []

    def findServer(self, opr, bankId):
        if (opr == 0):
            for bank in self.config['banks']:
                if (bankId == bank['bankId']):
                    return bank['tailServer']
        else:
            for bank in self.config['banks']:
                if (bankId == bank['bankId']):
                    return bank['headServer']

    def _Client_handler_4(self, res):
        self.output(((('ClientId: ' + str(self.clientId)) + ' Received response from server for request: ') + str(res['reqId'])))
        self.output(((('ClientId: ' + str(self.clientId)) + ' Current Balance: ') + str(res['currBal'])))
        self.responses.append(res)
        self.lastRecv = res['reqId']
        self.numsLR = self.lastRecv.split('.')
        self.output(('last received updated: ' + self.lastRecv))
        self.output(('Responses: ' + json.dumps(self.responses)))
    _Client_handler_4._labels = None
    _Client_handler_4._notlabels = None

def countProcesses(config):
    count = {}
    servers = 0
    for c in config:
        for bank in c['bank']:
            servers += len(bank['servers'])
        count['total_servers'] = servers
        count['total_clients'] = len(c['client'])
    print(('Bootstraping: Calculating #  of processes: ' + json.dumps(count)))
    return count

def main():
    da.api.config(clock='Lamport')
    print('Bootstraping: loading and parsing the config file')
    dataFile = open('/home/ppandey/async/cse535/chain_rep_distalgo/randomPayload.json')
    data = json.load(dataFile, cls=ConcatJSONDecoder)
    cfgFile = open('/home/ppandey/async/cse535/chain_rep_distalgo/config.json')
    config = json.load(cfgFile, cls=ConcatJSONDecoder)
    count = countProcesses(config)
    servers = da.api.new(Server, num=count['total_servers'])
    clients = da.api.new(Client, num=count['total_clients'])
    clientMap = []
    for c in config:
        for client in c['client']:
            clientMap.append(client)
    serverMap = []
    for c in config:
        for bank in c['bank']:
            for s in bank['servers']:
                conf = {}
                conf['bankId'] = bank['bankId']
                conf['type'] = s['type']
                conf['serverId'] = s['serverId']
                serverMap.append(conf)
    print('Bootstraping: Setting up client/server processes')
    i = 0
    serList = list(servers)
    for (process, config) in zip(serList, serverMap):
        if (config['type'] == 0):
            da.api.setup({process}, (clients, config, None, serList[(i + 1)]))
        elif (config['type'] == 2):
            da.api.setup({process}, (clients, config, serList[(i - 1)], None))
        else:
            da.api.setup({process}, (clients, config, serList[(i - 1)], serList[(i + 1)]))
        i += 1
    cltList = list(clients)
    for (process, config) in zip(cltList, clientMap):
        da.api.setup({process}, (servers, config, data))
    print('Bootstraping: Starting client/server processes')
    da.api.start(servers)
    da.api.start(clients)
