
import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('Query'), da.pat.FreePattern('req')])
PatternExpr_1 = da.pat.FreePattern('p')
PatternExpr_3 = da.pat.TuplePattern([da.pat.ConstantPattern('Query'), da.pat.FreePattern('req')])
PatternExpr_4 = da.pat.FreePattern('p')
PatternExpr_5 = da.pat.TuplePattern([da.pat.ConstantPattern('Pong')])
PatternExpr_6 = da.pat.FreePattern('rclk')
PatternExpr_8 = da.pat.TuplePattern([da.pat.ConstantPattern('Response'), da.pat.FreePattern('res')])
import sys
import json

class Server(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._ServerReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_0', PatternExpr_0, sources=[PatternExpr_1], destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_1', PatternExpr_3, sources=[PatternExpr_4], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_0])])

    def main(self):
        _st_label_8 = 0
        while (_st_label_8 == 0):
            _st_label_8 += 1
            if (len([p for (_, (_, _, p), (_ConstantPattern11_, req)) in self._ServerReceivedEvent_0 if (_ConstantPattern11_ == 'Query')]) == self.total_pings):
                _st_label_8 += 1
            else:
                super()._label('_st_label_8', block=True)
                _st_label_8 -= 1

    def setup(self, total_pings, serverId):
        self.serverId = serverId
        self.total_pings = total_pings
        self.accDetails = {}

    def _Server_handler_0(self, req, p):
        self.output(json.dumps(req))
        self.output(('Received request from client: ' + str(req['clientId'])))
        num = req['accNum']
        res = {}
        res['reqId'] = req['reqId']
        res['outcome'] = 0
        if (num in self.accDetails):
            res['currBal'] = self.accDetails[num]
        else:
            self.output('Account does not exists. Creating new account')
            self.accDetails[num] = 0
            res['currBal'] = 0
        self._send(('Response', res), p)
    _Server_handler_0._labels = None
    _Server_handler_0._notlabels = None

class Client(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._ClientReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_0', PatternExpr_5, sources=None, destinations=None, timestamps=[PatternExpr_6], record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_1', PatternExpr_8, sources=None, destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_1])])

    def main(self):
        for i in range(self.nrounds):
            clk = self.logical_clock()
            req = {'reqId': 1, 'opr': 'bal', 'clientId': 100, 'accNum': 1000}
            self.output('Sending request to server')
            self._send(('Query', req), self.p)
            rclk = None

            def ExistentialOpExpr_0():
                nonlocal rclk
                for (_, (rclk, _, _), (_ConstantPattern27_,)) in self._ClientReceivedEvent_0:
                    if (_ConstantPattern27_ == 'Pong'):
                        if (rclk > clk):
                            return True
                return False
            _st_label_31 = 0
            while (_st_label_31 == 0):
                _st_label_31 += 1
                if ExistentialOpExpr_0():
                    _st_label_31 += 1
                else:
                    super()._label('_st_label_31', block=True)
                    _st_label_31 -= 1
            else:
                if (_st_label_31 != 2):
                    continue
            if (_st_label_31 != 2):
                break

    def setup(self, p, nrounds):
        self.p = p
        self.nrounds = nrounds
        pass

    def _Client_handler_1(self, res):
        self.output('Received response from server')
        self.output(('Current Balance: ' + str(res['currBal'])))
    _Client_handler_1._labels = None
    _Client_handler_1._notlabels = None

def main():
    da.api.config(clock='Lamport')
    server = da.api.new(Server, [1, 1], num=1)
    client = da.api.new(Client, [server, 1], num=1)
    da.api.start(server)
    da.api.start(client)
