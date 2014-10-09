
import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('Query'), da.pat.FreePattern('req')])
PatternExpr_1 = da.pat.FreePattern('p')
PatternExpr_2 = da.pat.TuplePattern([da.pat.ConstantPattern('Update'), da.pat.FreePattern('req')])
PatternExpr_3 = da.pat.FreePattern('p')
PatternExpr_4 = da.pat.TuplePattern([da.pat.ConstantPattern('Response'), da.pat.FreePattern('res')])
PatternExpr_6 = da.pat.TuplePattern([da.pat.ConstantPattern('Response'), da.pat.FreePattern('res')])
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
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_0', PatternExpr_0, sources=[PatternExpr_1], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_0]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_1', PatternExpr_2, sources=[PatternExpr_3], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_1])])

    def main(self):
        _st_label_23 = 0
        while (_st_label_23 == 0):
            _st_label_23 += 1
            if (self.cnt == self.total_pings):
                _st_label_23 += 1
            else:
                super()._label('_st_label_23', block=True)
                _st_label_23 -= 1

    def setup(self, total_pings, serverId):
        self.total_pings = total_pings
        self.serverId = serverId
        self.cnt = 0
        self.accDetails = {}

    def _Server_handler_0(self, p, req):
        self.output(json.dumps(req))
        self.output(((('Received request: ' + str(req['reqId'])) + ' from client: ') + str(req['clientId'])))
        num = req['accNum']
        res = {}
        res['reqId'] = req['reqId']
        res['outcome'] = 'Processed'
        if (num in self.accDetails):
            res['currBal'] = self.accDetails[num]
        else:
            self.output('Account does not exists. Creating new account')
            self.accDetails[num] = 0
            res['currBal'] = 0
        self._send(('Response', res), p)
    _Server_handler_0._labels = None
    _Server_handler_0._notlabels = None

    def _Server_handler_1(self, p, req):
        self.output(json.dumps(req))
        self.output(((('Received request: ' + str(req['reqId'])) + ' from client: ') + str(req['clientId'])))
        num = req['accNum']
        amt = req['amount']
        res = {}
        res['reqId'] = req['reqId']
        if (num in self.accDetails):
            bal = self.accDetails[num]
            if (req['opr'] == 1):
                self.accDetails[num] = (bal + amt)
                self.output(('Updating the bal: ' + str((bal + num))))
                res['outcome'] = 'Processed'
            elif (req['opr'] == 2):
                if (bal < amt):
                    self.output('Not sufficient balance')
                    res['outcome'] = 'InsufficientBalance'
                else:
                    self.accDetails[num] = (bal - amt)
                    res['outcome'] = 'Processed'
        else:
            self.output('Account does not exists. Creating new account')
            self.accDetails[num] = amt
            res['outcome'] = 'Processed'
        res['currBal'] = self.accDetails[num]
        self._send(('Response', res), p)
    _Server_handler_1._labels = None
    _Server_handler_1._notlabels = None

class Client(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._ClientReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_0', PatternExpr_4, sources=None, destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_1', PatternExpr_6, sources=None, destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_2])])

    def main(self):
        for d in self.data:
            for item in d['data']:
                clientId = item['clientId']
                for payload in item['payloads']:
                    req = payload['payload']
                    req['clientId'] = clientId
                    reqId = req['reqId']
                    type = ''
                    if (req['operation'] == 0):
                        type = 'Query'
                    else:
                        type = 'Update'
                    clk = self.logical_clock()
                    self.output(('Sending request to server: ' + str(req['reqId'])))
                    self._send((type, req), self.p)
                    res = None

                    def ExistentialOpExpr_0():
                        nonlocal res
                        for (_, _, (_ConstantPattern18_, res)) in self._ClientReceivedEvent_0:
                            if (_ConstantPattern18_ == 'Response'):
                                if (res['reqId'] == (reqId - 1)):
                                    return True
                        return False
                    _st_label_79 = 0
                    while (_st_label_79 == 0):
                        _st_label_79 += 1
                        if ExistentialOpExpr_0():
                            _st_label_79 += 1
                        else:
                            super()._label('_st_label_79', block=True)
                            _st_label_79 -= 1
                    else:
                        if (_st_label_79 != 2):
                            continue
                    if (_st_label_79 != 2):
                        break

    def setup(self, p, data):
        self.p = p
        self.data = data
        pass

    def _Client_handler_2(self, res):
        self.output(('Received response from server for request: ' + str(res['reqId'])))
        self.output(('Current Balance: ' + str(res['currBal'])))
    _Client_handler_2._labels = None
    _Client_handler_2._notlabels = None

def main():
    da.api.config(clock='Lamport')
    payload = open('/home/ppandey/async/cse535/chain_rep_distalgo/payload.json')
    data = json.load(payload, cls=ConcatJSONDecoder)
    da.api.config(clock='Lamport')
    server = da.api.new(Server, [1, 4], num=1)
    client = da.api.new(Client, [server, data], num=1)
    da.api.start(server)
    da.api.start(client)
