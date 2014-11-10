
import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('extendChain'), da.pat.FreePattern('req')])
PatternExpr_1 = da.pat.FreePattern('p')
PatternExpr_2 = da.pat.TuplePattern([da.pat.ConstantPattern('Query'), da.pat.FreePattern('req')])
PatternExpr_3 = da.pat.FreePattern('p')
PatternExpr_4 = da.pat.TuplePattern([da.pat.ConstantPattern('Update'), da.pat.FreePattern('req')])
PatternExpr_5 = da.pat.FreePattern('p')
PatternExpr_6 = da.pat.TuplePattern([da.pat.ConstantPattern('Sync'), da.pat.FreePattern('req')])
PatternExpr_7 = da.pat.FreePattern('p')
PatternExpr_8 = da.pat.TuplePattern([da.pat.ConstantPattern('Ack'), da.pat.FreePattern('reqId'), da.pat.FreePattern('serverId')])
PatternExpr_9 = da.pat.FreePattern('p')
PatternExpr_10 = da.pat.TuplePattern([da.pat.ConstantPattern('CheckLog'), da.pat.FreePattern('req')])
PatternExpr_11 = da.pat.FreePattern('p')
PatternExpr_12 = da.pat.TuplePattern([da.pat.ConstantPattern('Failure'), da.pat.FreePattern('payload')])
PatternExpr_13 = da.pat.FreePattern('p')
PatternExpr_14 = da.pat.TuplePattern([da.pat.ConstantPattern('Response'), da.pat.FreePattern('res')])
PatternExpr_15 = da.pat.FreePattern('p')
PatternExpr_16 = da.pat.TuplePattern([da.pat.ConstantPattern('CheckLog'), da.pat.FreePattern('res')])
PatternExpr_17 = da.pat.FreePattern('p')
PatternExpr_18 = da.pat.TuplePattern([da.pat.ConstantPattern('extendChain'), da.pat.FreePattern('res')])
PatternExpr_19 = da.pat.FreePattern('p')
PatternExpr_20 = da.pat.TuplePattern([da.pat.ConstantPattern('HeartBeat'), da.pat.FreePattern('server')])
PatternExpr_21 = da.pat.FreePattern('p')
PatternExpr_22 = da.pat.TuplePattern([da.pat.ConstantPattern('PredUpdated'), da.pat.FreePattern('data')])
PatternExpr_23 = da.pat.FreePattern('p')
PatternExpr_24 = da.pat.TuplePattern([da.pat.ConstantPattern('AddToChain'), da.pat.FreePattern('data')])
PatternExpr_25 = da.pat.FreePattern('p')
PatternExpr_26 = da.pat.TuplePattern([da.pat.ConstantPattern('Failure'), da.pat.FreePattern('res')])
PatternExpr_27 = da.pat.FreePattern('p')
import sys
import json
import pickle
import re
import itertools
import heapq
import time
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
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_0', PatternExpr_0, sources=[PatternExpr_1], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_0]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_1', PatternExpr_2, sources=[PatternExpr_3], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_1]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_2', PatternExpr_4, sources=[PatternExpr_5], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_2]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_3', PatternExpr_6, sources=[PatternExpr_7], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_3]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_4', PatternExpr_8, sources=[PatternExpr_9], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_4]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_5', PatternExpr_10, sources=[PatternExpr_11], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_5]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_6', PatternExpr_12, sources=[PatternExpr_13], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_6])])

    def main(self):
        self.output((('ServerId: ' + str(self.serverId)) + ' starting the server operations'))
        heartBeatMsg = {}
        heartBeatMsg['serverId'] = self.serverId
        heartBeatMsg['bankId'] = self.config['bankId']
        heartBeatMsg['type'] = self.config['type']
        bankId = self.config['bankId']
        _st_label_41 = 0
        self._timer_start()
        while (_st_label_41 == 0):
            _st_label_41 += 1
            if False:
                pass
                _st_label_41 += 1
            elif self._timer_expired:
                pass
                _st_label_41 += 1
            else:
                super()._label('_st_label_41', block=True, timeout=self.startupDelay)
                _st_label_41 -= 1
        if (self.extendChain == 1):
            self.contactMaster(bankId)
        while True:
            _st_label_47 = 0
            self._timer_start()
            while (_st_label_47 == 0):
                _st_label_47 += 1
                if False:
                    pass
                    _st_label_47 += 1
                elif self._timer_expired:
                    self.sendHeartBeatMsg(heartBeatMsg)
                    _st_label_47 += 1
                else:
                    super()._label('_st_label_47', block=True, timeout=2)
                    _st_label_47 -= 1
            else:
                if (_st_label_47 != 2):
                    continue
            if (_st_label_47 != 2):
                break

    def setup(self, clients, master, config, pred, succ):
        self.clients = clients
        self.config = config
        self.master = master
        self.pred = pred
        self.succ = succ
        self.serverId = config['serverId']
        self.startupDelay = config['startupDelay']
        self.serverLifeTime = config['serverLifeTime']
        self.extendChain = config['extendChain']
        self.accDetails = {}
        self.history = {}
        self.sentReq = []
        self.clientProcessList = list(clients)
        self.totalSentCnt = 0
        self.totalRecvCnt = 0
        self.lastSeqNum = 0

    def contactMaster(self, bankId):
        self.output((('ServerId: ' + str(self.serverId)) + ' contacting master to extend chain'))
        payload = {}
        payload['bankId'] = bankId
        payload['serverId'] = self.serverId
        data = {'extendChain': payload}
        self._send(('AddToChain', data), self.master)

    def sendHeartBeatMsg(self, msg):
        if (not ('UNBOUND' in self.serverLifeTime)):
            if (('RecvNum' in self.serverLifeTime.keys()) and (self.serverLifeTime['RecvNum'] == self.totalRecvCnt)):
                self.output((('ServerId: ' + str(self.serverId)) + ' RECV request limit reached. Terminating server'))
                sys.exit()
            elif (('SentNum' in self.serverLifeTime.keys()) and (self.serverLifeTime['SentNum'] == self.totalSentCnt)):
                self.output((('ServerId: ' + str(self.serverId)) + ' SEND request limit reached. Terminating server'))
                sys.exit()
        self.output((('ServerId: ' + str(self.serverId)) + ' sending heart beat to master'))
        self._send(('HeartBeat', msg), self.master)

    def handleNewSucc(self, seqNum):
        i = 0
        flag = False
        for item in self.sentReq:
            i += 1
            if (item['reqId'] == seqNum):
                flag = True
                break
        if (flag == True):
            for j in range(i, len(self.sentReq)):
                self._send(('Sync', self.sentReq[j]), self.succ)
        else:
            for j in range(0, len(self.sentReq)):
                self._send(('Sync', self.sentReq[j]), self.succ)

    def _Server_handler_0(self, req, p):
        if req['serverId']:
            self.serverId = req['serverId']
        if ('predecessor' in req.keys()):
            self.pred = req['predecessor']
        if (req['type'] == 2):
            serverType = 2
            self.output((('ServerId: ' + str(self.serverId)) + ' activating as new tail and updated the predecessor'))
            res = {}
            res['ack'] = 1
            self._send(('PredUpdated', res), self.master)
        elif (req['type'] == 1):
            self.output((('ServerId: ' + str(self.serverId)) + ' Updating new successor and sync data with new tail'))
            serverType = 1
            self.succ = req['successor']
            data = {}
            data['extendChain'] = 3
            data['accDetails'] = self.accDetails
            data['sentReq'] = self.sentReq
            data['history'] = self.history
            self._send(('extendChain', data), serverList[self.succ])
        if (req['extendChain'] == 3):
            self.accDetails = data['accDetails']
            self.sentReq = data['sentReq']
            self.history = data['history']
            res = {}
            res['ack'] = 2
            self.output((('ServerId: ' + str(self.serverId)) + ' Sync completed with old tail'))
            self._send(('SyncComplete', res), self.master)
        if (req['extendChain'] == (- 1)):
            serverType = 2
            self.output((('ServerId: ' + str(self.serverId)) + ' OLd tail reveretd'))
    _Server_handler_0._labels = None
    _Server_handler_0._notlabels = None

    def _Server_handler_1(self, req, p):
        self.output(((((('ServerId: ' + str(self.serverId)) + ' Received Query request: ') + str(req['reqId'])) + ' from client: ') + str(req['clientId'])))
        self.output(((('ServerId: ' + str(self.serverId)) + ' ') + json.dumps(req)))
        num = req['accNum']
        reqId = req['reqId']
        res = {}
        flag = True
        self.totalRecvCnt += 1
        if (reqId in self.history):
            flag = False
            hist = self.history[reqId]
            if (hist['payload']['accNum'] == num):
                res = hist['response']
                self.output(((('ServerId: ' + str(self.serverId)) + ' Query request already exists in history: ') + json.dumps(res)))
            else:
                res['reqId'] = reqId
                res['outcome'] = 'InconsistentWithHistory'
                res['accNum'] = num
                res['currBal'] = 0
                self.output(((('ServerId: ' + str(self.serverId)) + ' Query request already inconsistent with history: ') + json.dumps(res)))
        else:
            res['reqId'] = reqId
            res['outcome'] = 'Processed'
            res['accNum'] = num
            if (num in self.accDetails):
                res['currBal'] = self.accDetails[num]
            else:
                self.output((('ServerId: ' + str(self.serverId)) + ' Account does not exists. Creating new account'))
                res['currBal'] = 0
        if flag:
            hist = {}
            hist['payload'] = req
            hist['response'] = res
            self.history[reqId] = hist
        if (req['simFail'] == 2):
            self.output((('ServerId: ' + str(self.serverId)) + ' Simulating msg drop from server-client.'))
            return
        self._send(('Response', res), p)
        self.totalSentCnt += 1
    _Server_handler_1._labels = None
    _Server_handler_1._notlabels = None

    def _Server_handler_2(self, req, p):
        self.output(((((('ServerId: ' + str(self.serverId)) + ' Received Update request: ') + str(req['reqId'])) + ' from client: ') + str(req['clientId'])))
        self.output(((('ServerId: ' + str(self.serverId)) + ' ') + json.dumps(req)))
        num = req['accNum']
        amt = req['amount']
        oper = req['operation']
        reqId = req['reqId']
        res = {}
        flag = True
        self.totalRecvCnt += 1
        if (reqId in self.history):
            flag = False
            hist = self.history[reqId]
            if ((hist['payload']['accNum'] == num) and (hist['payload']['operation'] == oper) and (hist['payload']['amount'] == amt)):
                res = hist['response']
                self.output(((('ServerId: ' + str(self.serverId)) + ' Update request already exists in history: ') + json.dumps(res)))
            else:
                res['reqId'] = reqId
                res['outcome'] = 'InconsistentWithHistory'
                res['accNum'] = num
                res['currBal'] = 0
                self.output(((('ServerId: ' + str(self.serverId)) + ' Update request inconsistent with history: ') + json.dumps(res)))
            if (req['simFail'] == 2):
                self.output((('ServerId: ' + str(self.serverId)) + ' Simulating msg drop from server-client.'))
            else:
                self._send(('Response', res), p)
                self.totalSentCnt += 1
        else:
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
                        res['outcome'] = 'InSufficientFunds'
                    else:
                        self.accDetails[num] = (bal - amt)
                        res['outcome'] = 'Processed'
            else:
                self.output((('ServerId: ' + str(self.serverId)) + ' Account does not exists. Creating new account'))
                if (req['operation'] == 1):
                    self.accDetails[num] = amt
                    res['outcome'] = 'Processed'
                else:
                    self.accDetails[num] = 0
                    res['outcome'] = 'InSufficientFunds'
            res['currBal'] = self.accDetails[num]
            res['payload'] = req
        if flag:
            hist = {}
            hist['payload'] = req
            hist['response'] = res
            self.history[reqId] = hist
            self.sentReq.append(res)
            self._send(('Sync', res), self.succ)
            self.totalSentCnt += 1
    _Server_handler_2._labels = None
    _Server_handler_2._notlabels = None

    def _Server_handler_3(self, req, p):
        self.output(((('ServerId: ' + str(self.serverId)) + ' Received Sync request: ') + str(req['reqId'])))
        self.output(((('ServerId: ' + str(self.serverId)) + ' ') + json.dumps(req)))
        num = req['payload']['accNum']
        reqId = req['reqId']
        clientId = req['payload']['clientId']
        self.accDetails[num] = req['currBal']
        self.history[reqId] = {'payload': req['payload'], 'response': req}
        self.sentReq.append(req)
        self.totalRecvCnt += 1
        if (self.config['type'] == 2):
            client = self.clientProcessList[clientId]
            if (req['payload']['simFail'] == 2):
                self.output((('ServerId: ' + str(self.serverId)) + ' Simulating msg drop from server-client.'))
            else:
                res = req
                del res['payload']
                self._send(('Response', res), client)
            self._send(('Ack', reqId, self.serverId), self.pred)
        else:
            self._send(('Sync', req), self.succ)
        self.totalSentCnt += 1
    _Server_handler_3._labels = None
    _Server_handler_3._notlabels = None

    def _Server_handler_4(self, serverId, reqId, p):
        self.output(((('ServerId: ' + str(serverId)) + ' Received Ack request: ') + str(reqId)))
        for item in self.sentReq:
            if (item['reqId'] == reqId):
                del item
    _Server_handler_4._labels = None
    _Server_handler_4._notlabels = None

    def _Server_handler_5(self, p, req):
        reqId = req['reqId']
        res = {}
        self.output(((('ServerId: ' + str(self.serverId)) + ' Processing check log for req: ') + req['reqId']))
        if (reqId in self.history.keys()):
            res = self.history[reqId]['response']
            res['checkLog'] = 1
        else:
            res['checkLog'] = 0
            res['reqId'] = reqId
        self.output(((('ServerId: ' + str(self.serverId)) + ' Processed check log for req: ') + req['reqId']))
        self._send(('CheckLog', res), p)
    _Server_handler_5._labels = None
    _Server_handler_5._notlabels = None

    def _Server_handler_6(self, payload, p):
        self.output((('ServerId: ' + str(self.serverId)) + ' handling the server failure'))
        server = payload['failure']['server']
        type = payload['failure']['type']
        if (type == 'head'):
            self.config['type'] = 0
            self.output((('ServerId: ' + str(self.serverId)) + ' updated the server type to HEAD'))
        elif (type == 'tail'):
            self.config['type'] = 2
            self.output((('ServerId: ' + str(self.serverId)) + ' updated the server type to TAIL'))
        elif (type == 'successor'):
            self.succ = server
            self.output((('ServerId: ' + str(self.serverId)) + ' updated the successor server'))
            self.handleNewSucc(payload['failure']['seqNum'])
        elif (type == 'predecessor'):
            self.pred = server
            res = {'seqNum': self.lastSeqNum}
            self._send(('Failure', res), p)
    _Server_handler_6._labels = None
    _Server_handler_6._notlabels = None

class Client(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_0', PatternExpr_14, sources=[PatternExpr_15], destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_7]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_1', PatternExpr_16, sources=[PatternExpr_17], destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_8]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_2', PatternExpr_18, sources=[PatternExpr_19], destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_9])])

    def main(self):
        self.output((('ClientId: ' + str(self.clientId)) + ' starting the client operations'))
        for d in self.data:
            for item in d['data']:
                if (self.clientId == item['clientId']):
                    for payload in item['payloads']:
                        _st_label_284 = 0
                        self._timer_start()
                        while (_st_label_284 == 0):
                            _st_label_284 += 1
                            if False:
                                pass
                                _st_label_284 += 1
                            elif self._timer_expired:
                                pass
                                _st_label_284 += 1
                            else:
                                super()._label('_st_label_284', block=True, timeout=1)
                                _st_label_284 -= 1
                        else:
                            if (_st_label_284 != 2):
                                continue
                        if (_st_label_284 != 2):
                            break
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
                            while self.retryLimit:
                                _st_label_298 = 0
                                self._timer_start()
                                while (_st_label_298 == 0):
                                    _st_label_298 += 1
                                    if (self.preReq['reqId'] in self.responses):
                                        self.retryLimit = False
                                        _st_label_298 += 1
                                    elif self._timer_expired:
                                        self.tryResending(self.preReq, type)
                                        _st_label_298 += 1
                                    else:
                                        super()._label('_st_label_298', block=True, timeout=int(self.resendDelay))
                                        _st_label_298 -= 1
                                else:
                                    if (_st_label_298 != 2):
                                        continue
                                if (_st_label_298 != 2):
                                    break
                        self.retryLimit = True
                        idx = self.findServer(req['operation'], req['bankId'])
                        p = self.serverProcessList[idx]
                        self.output(((((('ClientId: ' + str(self.clientId)) + ' Sending request:') + str(req['reqId'])) + ' to server: ') + str(idx)))
                        if (req['simFail'] == 1):
                            self.output((('ClientId: ' + str(self.clientId)) + ' Simulating message failure between client-server'))
                        else:
                            self._send((type, req), p)
                        self.preReq = req
                        self.currDelay = int(time.time())
                        self.currRetriesCnt = 1
                    while self.retryLimit:
                        _st_label_312 = 0
                        self._timer_start()
                        while (_st_label_312 == 0):
                            _st_label_312 += 1
                            if (self.preReq['reqId'] in self.responses):
                                pass
                                _st_label_312 += 1
                            elif self._timer_expired:
                                self.tryResending(self.preReq, type)
                                _st_label_312 += 1
                            else:
                                super()._label('_st_label_312', block=True, timeout=self.resendDelay)
                                _st_label_312 -= 1
                        else:
                            if (_st_label_312 != 2):
                                continue
                        if (_st_label_312 != 2):
                            break
                    self.retryLimit = True
        _st_label_316 = 0
        while (_st_label_316 == 0):
            _st_label_316 += 1
            if False:
                _st_label_316 += 1
            else:
                super()._label('_st_label_316', block=True)
                _st_label_316 -= 1

    def setup(self, servers, config, data):
        self.config = config
        self.servers = servers
        self.data = data
        self.clientId = config['clientId']
        self.resendDelay = config['resendDelay']
        self.numRetries = config['numRetries']
        self.serverProcessList = list(servers)
        self.preReq = {}
        self.responses = {}
        self.currDelay = 0
        self.currRetriesCnt = 0
        self.retryLimit = True
        self.checkLogFlag = (- 1)
        self.extendChainSleepFlag = (- 1)

    def findServer(self, opr, bankId):
        if ((opr == 0) or (opr == 4)):
            for bank in self.config['banks']:
                if (bankId == bank['bankId']):
                    return bank['tailServer']
        else:
            for bank in self.config['banks']:
                if (bankId == bank['bankId']):
                    return bank['headServer']

    def tryResending(self, preReq, type):
        _st_label_323 = 0
        while (_st_label_323 == 0):
            _st_label_323 += 1
            if (self.extendChainSleepFlag == 0):
                pass
                _st_label_323 += 1
            else:
                super()._label('_st_label_323', block=True)
                _st_label_323 -= 1
        self.extendChainSleepFlag = (- 1)
        self.output(((('ClientId: ' + str(self.clientId)) + ' Request timed out. ') + str(preReq['reqId'])))
        currTime = int(time.time())
        if ((currTime - self.currDelay) > self.resendDelay):
            if (self.currRetriesCnt < self.numRetries):
                self.output(((('ClientId: ' + str(self.clientId)) + ' Checking with tail whether operation already performed: ') + str(preReq['reqId'])))
                self.data = {'checkLog': 1, 'reqId': preReq['reqId']}
                idx = self.findServer(4, preReq['bankId'])
                p = self.serverProcessList[idx]
                self._send(('CheckLog', preReq), p)
                _st_label_335 = 0
                self._timer_start()
                while (_st_label_335 == 0):
                    _st_label_335 += 1
                    if (self.checkLogFlag == (- 1)):
                        pass
                        _st_label_335 += 1
                    elif self._timer_expired:
                        pass
                        _st_label_335 += 1
                    else:
                        super()._label('_st_label_335', block=True, timeout=5)
                        _st_label_335 -= 1
                if (self.checkLogFlag == 0):
                    self.output(((('ClientId: ' + str(self.clientId)) + ' Request not performed at the tail: ') + str(preReq['reqId'])))
                    self.output(((('ClientId: ' + str(self.clientId)) + ' Performing request again: ') + str(preReq['reqId'])))
                    self.checkLogFlag = (- 1)
                    idx = self.findServer(preReq['operation'], preReq['bankId'])
                    p = self.serverProcessList[idx]
                    if (preReq['simFail'] == 1):
                        self.output((('ClientId: ' + str(self.clientId)) + ' Simulating message failure between client-server'))
                    else:
                        self.output(((('ClientId: ' + str(self.clientId)) + ' Resending request: ') + str(preReq['reqId'])))
                        self._send((type, preReq), p)
                    self.currDelay = int(time.time())
                    self.currRetriesCnt += 1
                elif (self.checkLogFlag == 1):
                    self.checkLogFlag = (- 1)
            else:
                self.retryLimit = False
                self.output(((((((('ClientId: ' + str(self.clientId)) + ' Number of retries ') + str(self.currRetriesCnt)) + ' exceeded the limit ') + str(self.numRetries)) + ' Aborting request ') + str(preReq['reqId'])))

    def _Client_handler_7(self, res, p):
        self.output(((('ClientId: ' + str(self.clientId)) + ' Received response from server for request: ') + str(res['reqId'])))
        self.output(((('ClientId: ' + str(self.clientId)) + ' Current Balance: ') + str(res['currBal'])))
        self.responses[res['reqId']] = res
        self.output(((('ClientId: ' + str(self.clientId)) + ' Responses: ') + json.dumps(self.responses)))
    _Client_handler_7._labels = None
    _Client_handler_7._notlabels = None

    def _Client_handler_8(self, p, res):
        self.output(((('ClientId: ' + str(self.clientId)) + ' Received check log response ') + json.dumps(res)))
        self.checkLogFlag = res['checkLog']
        if (self.checkLogFlag == 1):
            self.output(((('ClientId: ' + str(self.clientId)) + ' Received response from server for request: ') + str(res['reqId'])))
            self.output(((('ClientId: ' + str(self.clientId)) + ' Current Balance: ') + str(res['currBal'])))
            self.responses[res['reqId']] = res
            self.output(((('ClientId: ' + str(self.clientId)) + ' Responses: ') + json.dumps(self.responses)))
    _Client_handler_8._labels = None
    _Client_handler_8._notlabels = None

    def _Client_handler_9(self, res, p):
        if (res['extendChain']['flag'] == 0):
            self.extendChainSleepFlag = 0
        elif (res['extendChain']['flag'] == 1):
            self.extendChainSleepFlag = 1
        else:
            self.extendChainSleepFlag = (- 1)
    _Client_handler_9._labels = None
    _Client_handler_9._notlabels = None

class Master(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_0', PatternExpr_20, sources=[PatternExpr_21], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_10]), da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_1', PatternExpr_22, sources=[PatternExpr_23], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_11]), da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_2', PatternExpr_24, sources=[PatternExpr_25], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_12]), da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_3', PatternExpr_26, sources=[PatternExpr_27], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_13])])

    def main(self):
        self.output('Master: Master process started')
        while True:
            _st_label_381 = 0
            self._timer_start()
            while (_st_label_381 == 0):
                _st_label_381 += 1
                if False:
                    pass
                    _st_label_381 += 1
                elif self._timer_expired:
                    self.probeServerFailure()
                    _st_label_381 += 1
                else:
                    super()._label('_st_label_381', block=True, timeout=1)
                    _st_label_381 -= 1
            else:
                if (_st_label_381 != 2):
                    continue
            if (_st_label_381 != 2):
                break

    def setup(self, bankClientMap, bankServerMap, heartBeatDelay, clients, servers):
        self.clients = clients
        self.bankServerMap = bankServerMap
        self.heartBeatDelay = heartBeatDelay
        self.servers = servers
        self.bankClientMap = bankClientMap
        self.serverTimeStampMap = {}
        self.serverTimeStampHeap = []
        self.REMOVED = '<removed-task>'
        self.counter = itertools.count()
        self.clientList = list(clients)
        self.serverList = list(servers)
        self.succSeqNum = (- 1)
        self.extendChainFlag = (- 1)

    def addTimeStamp(self, server, timestamp=0):
        serverId = server['serverId']
        if (serverId in self.serverTimeStampMap):
            self.removeTimeStamp(server)
        count = next(self.counter)
        entry = [timestamp, count, server]
        self.serverTimeStampMap[server['serverId']] = entry
        heapq.heappush(self.serverTimeStampHeap, entry)

    def removeTimeStamp(self, server):
        entry = self.serverTimeStampMap.pop(server['serverId'])
        entry[(- 1)] = self.REMOVED

    def popTimeStamp(self):
        while self.serverTimeStampHeap:
            (timestamp, count, server) = heapq.heappop(self.serverTimeStampHeap)
            if (not (server is self.REMOVED)):
                del self.serverTimeStampMap[server['serverId']]
                return [server, timestamp]

    def probeServerFailure(self):
        serverObj = self.popTimeStamp()
        if (serverObj is None):
            return
        server = serverObj[0]
        timestamp = serverObj[1]
        if ((int(time.time()) - timestamp) > 5):
            self.output((('Master: ServerId: ' + str(server['serverId'])) + ' failed'))
            self.handleServerFailure(server)
        else:
            self.addTimeStamp(server, timestamp)

    def handleServerFailure(self, server):
        serverId = server['serverId']
        bankId = server['bankId']
        type = server['type']
        self.output(('Master: handling the server failure for ServerId: ' + str(serverId)))
        if (type == 0):
            newHead = self.updateChain(bankId, serverId, type)
            self.output(('Master: New head upated. ServerId: ' + str(newHead)))
            payload = {'failure': {'type': 'head', 'server': self.serverList[newHead], 'bankId': bankId}}
            self.notifyClient(bankId, payload)
            self._send(('Failure', payload), self.serverList[newHead])
        elif (type == 1):
            newSuccPred = self.updateChain(bankId, serverId, type)
            self.output(('Master: New relation: ' + json.dumps(newSuccPred)))
            payload = {'failure': {'type': 'predecessor', 'server': self.serverList[newSuccPred[0]]}}
            self._send(('Failure', payload), self.serverList[newSuccPred[1]])
            _st_label_430 = 0
            while (_st_label_430 == 0):
                _st_label_430 += 1
                if (not (self.succSeqNum == (- 1))):
                    _st_label_430 += 1
                else:
                    super()._label('_st_label_430', block=True)
                    _st_label_430 -= 1
            payload['failure']['type'] = 'successor'
            payload['failure']['server'] = self.serverList[newSuccPred[1]]
            payload['failure']['seqNum'] = self.succSeqNum
            self._send(('Failure', payload), self.serverList[newSuccPred[0]])
            self.succSeqNum = (- 1)
        elif (type == 2):
            newTail = self.updateChain(bankId, serverId, type)
            self.output(('Master: New Tail upated. ServerId: ' + str(newTail)))
            payload = {'failure': {'type': 'tail', 'server': self.serverList[newTail], 'bankId': bankId}}
            self.notifyClient(bankId, payload)
            self._send(('Failure', payload), self.serverList[newTail])
        else:
            self.output('Master: Error unknown server type')

    def notifyClient(self, bankId, payload):
        self.output('Master: entering notify clients.')
        for client in self.bankClientMap[bankId]:
            self.output(((('Master: Notifying client of bankId: ' + str(bankId)) + ' dest: ') + str(client)))
            if (not (payload['extendChain'] == None)):
                self.output('Master: Extend Chain operation being communicated with client.')
                self._send(('extendChain', payload), self.clientList[client])
            else:
                self.output('Master: Server failure being communicated with clients.')
                self._send(('Failure', payload), self.clientList[client])

    def updateChain(self, bankId, serverId, type):
        self.output(((('Master: Updating the chain for the bank: ' + str(bankId)) + ' server ') + str(serverId)))
        if (type == 0):
            del self.bankServerMap[bankId][0]
            return self.bankServerMap[bankId][0]
        elif (type == 1):
            i = 0
            for server in self.bankServerMap[bankId]:
                if (serverId == server):
                    del self.bankServerMap[bankId][server]
                    return [self.bankServerMap[bankId][(i - 1)], self.bankServerMap[bankId][i]]
                i += 1
        elif (type == 2):
            length = len(self.bankServerMap[bankId])
            del self.bankServerMap[bankId][(length - 1)]
            return self.bankServerMap[bankId][(length - 2)]
        self.output(('Master: Updated the chain for the bank: ' + str(bankId)))

    def _Master_handler_10(self, server, p):
        self.output(('Master: Received heart beat msg from server: ' + str(server['serverId'])))
        self.addTimeStamp(server, int(time.time()))
    _Master_handler_10._labels = None
    _Master_handler_10._notlabels = None

    def _Master_handler_11(self, data, p):
        if (data['ack'] == 1):
            self.extendChainFlag = 1
    _Master_handler_11._labels = None
    _Master_handler_11._notlabels = None

    def _Master_handler_12(self, data, p):
        payload = data['extendChain']
        serverId = payload['serverId']
        bankId = payload['bankId']
        self.output(((('Master: Adding new server for the bank' + str(bankId)) + ' to the list. Server Id ') + str(serverId)))
        data = {'extendChain': {'flag': 0}}
        self.notifyClient(bankId, data)
        self.servers = self.bankServerMap[bankId]
        for i in range(0, len(self.servers)):
            if (self.servers[i] == serverId):
                break
        pred = self.servers[(i - 1)]
        data = {'extendChain': 1, 'type': 2, 'predecessor': pred, 'serverId': serverId}
        self._send(('extendChain', data), self.serverList[serverId])
        _st_label_486 = 0
        self._timer_start()
        while (_st_label_486 == 0):
            _st_label_486 += 1
            if (self.extendChainFlag == (- 1)):
                pass
                _st_label_486 += 1
            elif self._timer_expired:
                self.output('Master: Cannot extend chain. The new server failed.')
                _st_label_486 += 1
            else:
                super()._label('_st_label_486', block=True, timeout=5)
                _st_label_486 -= 1
        if (self.extendChainFlag == 1):
            self.output('Master: New tail activated successfully.')
            data = {}
            data = {'extendChain': 2, 'type': 1, 'successor': serverId}
            self.output('Master: Notifying old tail of chain extension')
            self._send(('extendChain', data), self.serverList[pred])
            self.extendChainFlag = (- 1)
        _st_label_496 = 0
        self._timer_start()
        while (_st_label_496 == 0):
            _st_label_496 += 1
            if (self.extendChainFlag == (- 1)):
                pass
                _st_label_496 += 1
            elif self._timer_expired:
                self.output('Master: Cannot extend chain. The new server failed. Reverting back to the old chain')
                _st_label_496 += 1
            else:
                super()._label('_st_label_496', block=True, timeout=10)
                _st_label_496 -= 1
        if (self.extendChainFlag == (- 1)):
            data = {}
            data = {'extendChain': (- 1), 'type': 2, 'serverId': serverId}
            self.output('Master: Notifying of chain extension failure')
            self._send(('extendChain', data), self.serverList[pred])
        elif (self.extendChainFlag == 2):
            self.output('Master: New Tail synchronized successfully')
            self.bankServerMap[bankId].append(serverId)
            data = {'extendChain': {'flag': 1}}
            data['extendChain']['type'] = 2
            data['extendChain']['server'] = pred
            data['extendChain']['bankId'] = bankId
            self.notifyClient(bankId, data)
            self.extendChainFlag = (- 1)
    _Master_handler_12._labels = None
    _Master_handler_12._notlabels = None

    def _Master_handler_13(self, res, p):
        self.output('Master: received the seqNum from the successor')
        self.succSeqNum = res['seqNum']
    _Master_handler_13._labels = None
    _Master_handler_13._notlabels = None

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
    dataFile = open('/home/kavita/course_work/cse535/chain_rep_distalgo/payloadMsgDrop.json')
    data = json.load(dataFile, cls=ConcatJSONDecoder)
    cfgFile = open('/home/kavita/course_work/cse535/chain_rep_distalgo/config.json')
    config = json.load(cfgFile, cls=ConcatJSONDecoder)
    count = countProcesses(config)
    servers = da.api.new(Server, num=count['total_servers'])
    clients = da.api.new(Client, num=count['total_clients'])
    master = da.api.new(Master, num=1)
    clientMap = []
    for c in config:
        for client in c['client']:
            clientMap.append(client)
    serverMap = []
    bankServerMap = {}
    for c in config:
        for bank in c['bank']:
            serverList = []
            for s in bank['servers']:
                conf = {}
                serverList.append(s['serverId'])
                conf['bankId'] = bank['bankId']
                conf['type'] = s['type']
                conf['serverId'] = s['serverId']
                conf['startupDelay'] = s['startupDelay']
                conf['serverLifeTime'] = s['serverLifeTime']
                conf['extendChain'] = s['extendChain']
                serverMap.append(conf)
            bankServerMap[bank['bankId']] = serverList
    bankClientMap = {}
    for c in config:
        for bank in c['bank']:
            clientList = []
            for client in bank['clients']:
                clientList.append(client['clientId'])
            bankClientMap[bank['bankId']] = clientList
    heartBeatDelay = 0
    for c in config:
        m = c['master']
        heartBeatDelay = m['heartBeatDelay']
    print('Bootstraping: Setting up client/server processes')
    i = 0
    serList = list(servers)
    for (process, config) in zip(serList, serverMap):
        if (config['type'] == 0):
            da.api.setup({process}, (clients, master, config, None, serList[(i + 1)]))
        elif (config['type'] == 2):
            da.api.setup({process}, (clients, master, config, serList[(i - 1)], None))
        else:
            da.api.setup({process}, (clients, master, config, serList[(i - 1)], serList[(i + 1)]))
        i += 1
    cltList = list(clients)
    for (process, config) in zip(cltList, clientMap):
        da.api.setup({process}, (servers, config, data))
    da.api.setup(master, (bankClientMap, bankServerMap, heartBeatDelay, clients, servers))
    print('Bootstraping: Starting client/server processes')
    da.api.start(master)
    da.api.start(servers)
    da.api.start(clients)
