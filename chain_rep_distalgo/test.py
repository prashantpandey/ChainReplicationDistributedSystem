
import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('Ping')])
PatternExpr_1 = da.pat.FreePattern('p')
PatternExpr_3 = da.pat.TuplePattern([da.pat.ConstantPattern('Ping')])
PatternExpr_4 = da.pat.FreePattern('p')
PatternExpr_5 = da.pat.TuplePattern([da.pat.ConstantPattern('Pong')])
PatternExpr_6 = da.pat.FreePattern('rclk')
PatternExpr_8 = da.pat.TuplePattern([da.pat.ConstantPattern('Pong')])
import sys

class Pong(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._PongReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_PongReceivedEvent_0', PatternExpr_0, sources=[PatternExpr_1], destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_PongReceivedEvent_1', PatternExpr_3, sources=[PatternExpr_4], destinations=None, timestamps=None, record_history=None, handlers=[self._Pong_handler_0])])

    def main(self):
        _st_label_7 = 0
        while (_st_label_7 == 0):
            _st_label_7 += 1
            if (len([p for (_, (_, _, p), (_ConstantPattern10_,)) in self._PongReceivedEvent_0 if (_ConstantPattern10_ == 'Ping')]) == self.total_pings):
                _st_label_7 += 1
            else:
                super()._label('_st_label_7', block=True)
                _st_label_7 -= 1

    def setup(self, total_pings):
        self.total_pings = total_pings
        pass

    def _Pong_handler_0(self, p):
        self.output('Pinged')
        self._send(('Pong',), p)
    _Pong_handler_0._labels = None
    _Pong_handler_0._notlabels = None

class Ping(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._PingReceivedEvent_0 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_PingReceivedEvent_0', PatternExpr_5, sources=None, destinations=None, timestamps=[PatternExpr_6], record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_PingReceivedEvent_1', PatternExpr_8, sources=None, destinations=None, timestamps=None, record_history=None, handlers=[self._Ping_handler_1])])

    def main(self):
        for i in range(self.nrounds):
            clk = self.logical_clock()
            self._send(('Ping',), self.p)
            rclk = None

            def ExistentialOpExpr_0():
                nonlocal rclk
                for (_, (rclk, _, _), (_ConstantPattern24_,)) in self._PingReceivedEvent_0:
                    if (_ConstantPattern24_ == 'Pong'):
                        if (rclk > clk):
                            return True
                return False
            _st_label_18 = 0
            while (_st_label_18 == 0):
                _st_label_18 += 1
                if ExistentialOpExpr_0():
                    _st_label_18 += 1
                else:
                    super()._label('_st_label_18', block=True)
                    _st_label_18 -= 1
            else:
                if (_st_label_18 != 2):
                    continue
            if (_st_label_18 != 2):
                break

    def setup(self, p, nrounds):
        self.p = p
        self.nrounds = nrounds
        pass

    def _Ping_handler_1(self):
        self.output('Ponged.')
    _Ping_handler_1._labels = None
    _Ping_handler_1._notlabels = None

def main():
    da.api.config(clock='Lamport')
    pong = da.api.new(Pong, [1], num=1)
    ping = da.api.new(Ping, [pong, 1], num=1)
    da.api.start(pong)
    da.api.start(ping)
