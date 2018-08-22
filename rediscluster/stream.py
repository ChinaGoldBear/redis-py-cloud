# -*- coding: utf-8 -*-

class RedisStream(object):
    def xadd(self, key, id, maxlen=0, *field):
        pieces = [key]
        if maxlen:
            pieces.append("maxlen")
            pieces.append(str(maxlen))
        pieces.append(id)

        if not field or len(field) % 2 != 0:
            if len(field) > 0 and isinstance(field[0], dict):
                for k, v in field[0].items():
                    pieces.append(k)
                    pieces.append(v)
            else:
                raise "params lenght wrong."
        elif isinstance(field, tuple):
            for val in field:
                pieces.append(val)
        else:
            raise "params type wrong."
        return self.execute_command('XADD', *pieces)

    def xrange(self, key, start_id, end_id):
        pieces = [key, start_id, end_id]
        return self.execute_command('XRANGE', *pieces)

    def xlen(self, key):
        pieces = [key]
        return self.execute_command('XLEN', *pieces)

    def xread(self, key, id=None, count=1, block=None):
        pieces = ["count", count, "streams", key]
        if id != None:
            pieces.append(id)
        if block != None:
            pieces = ["block", block] + pieces
        return self.execute_command('XLEN', *pieces)

    def xreadgroup(self, group, consumer, count, streams, id=">", block=None):
        pieces = ["GROUP", group, consumer]
        if block != None:
            pieces += ["block", block]
        pieces += ["COUNT", count, "streams", streams, id]
        return self.execute_command('XREADGROUP', *pieces)

    def xack(self, streams, consumer, id):
        pieces = [streams, consumer, id]
        return self.execute_command('XACK', *pieces)
