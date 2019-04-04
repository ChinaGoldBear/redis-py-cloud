# -*- coding: utf-8 -*-
'''
power by ZHGoldBear
'''


class RedisStream(object):
    def xadd(self, key, id, maxlen=0, *field):
        '''
        XADD key ID field string [field string ...]
            Appends a new entry to a stream
        :param key:
            stream name
        :param id:
            id name. if not id then *.
        :param [maxlen]:
            max msg list lenght.
        :param date:
            {"name": "ZH","name2":"ZHH"}
            or
            name="ZH", name2="ZHH"

        example:
        # add one msg
        r.xadd("mystream", "*", {"name": "ZH"})
        # max len 100
        r.xadd("mystream", "*", 100,{"name": "ZH"})

        '''
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
        '''
        XRANGE key start end [COUNT count]
            Return a range of elements in a stream, with IDs matching the specified IDs interval
        :param key:
            stream name
        :param start_id:
            range start id
        :param end_id:
            range end id
        :return:
        '''
        pieces = [key, start_id, end_id]
        return self.execute_command('XRANGE', *pieces)

    def xlen(self, key):
        '''
        XLEN key
            Return the number of entires in a stream
        :param key:
            stream name
        :return:
        '''
        pieces = [key]
        return self.execute_command('XLEN', *pieces)

    def xread(self, key, id=None, count=1, block=None):
        """
        XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
            Return never seen elements in multiple streams, with IDs greater than the ones reported by the
            caller for each stream. Can block.
        :param key:
        :param id:
        :param count:
        :param block:
        :return:
        """
        pieces = ["count", count, "streams", key]
        if id != None:
            pieces.append(id)
        if block != None:
            pieces = ["block", block] + pieces
        return self.execute_command('XLEN', *pieces)

    def xreadgroup(self, group, consumer, count, streams, id=">", block=None):
        """
        XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
            Return new entries from a stream using a consumer group, or access the history of the pending
            entries for a given consumer. Can block.
        :param group:
        :param consumer:
        :param count:
        :param streams:
        :param id:
        :param block:
        :return:
        """
        pieces = ["GROUP", group, consumer]
        if block != None:
            pieces += ["block", block]
        pieces += ["COUNT", count, "streams", streams, id]
        return self.execute_command('XREADGROUP', *pieces)

    def xack(self, streams, consumer, id):
        """
        XACK streams, consumer, id
        :param streams:
        :param consumer:
        :param id:
        :return:
        """
        pieces = [streams, consumer, id]
        return self.execute_command('XACK', *pieces)

    def xgroup(self, streams, consumer, startpoint="0-0", commend="CREATE"):
        '''
        xgroup create streams consumer startpoint
        :param streams: streams name
        :param consumer: new consumer
        :param startpoint: 0-0 from head, $ from lastID, or streamsID
        :param commend: default
        :return:
        '''
        pieces = ["XGROUP", commend, streams, consumer, startpoint]
        return self.execute_command('XREADGROUP', *pieces)
