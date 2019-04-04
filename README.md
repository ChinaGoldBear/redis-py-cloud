# redis-py-cloud

This client provides a client for redis cluster that was added in Redis 5.0+. Support for stream data types.

This project is a port of `redis-py-cluster` by Johan Andersson, with alot of added functionality. The original source can be found at https://github.com/Grokzen/redis-py-cluster


# Documentation (NEWS)

Support for Stream of Redis5.0 new data types.

commend: xadd, xread, xreadgroup, xack, xack, xlen, xrange

All documentation (redis-py-cluster) can be found at http://redis-py-cluster.readthedocs.org/en/master

The redis-py-cloud documentation can be found at https://blog.csdn.net/copyangle/article/details/81975975



## Installation

```
$ pip install redis-py-cloud
```

```
$ git clone git@github.com:ChinaGoldBear/redis-py-cloud.git
$ python setup.py install
```



## Usage example

Small sample script that shows how to get started with RedisCluster. It can also be found in [examples/basic.py](examples/basic.py)

```python
>>> from rediscluster import StrictRedisCluster

>>> # Requires at least one node for cluster discovery. Multiple nodes is recommended.
>>> startup_nodes = [{"host": "127.0.0.1", "port": "7000"}]

>>> rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

>>> rc.set("foo", "bar")
True
>>> print(rc.get("foo"))
'bar'

>>> rc.xadd("mystream", "*", 100,{"name": "data"})

>>> rc.xread("mystream","*",1，0)

>>> rc.xrange("mystream","-","+")

>>> rc.xreadgroup("group_name","consumer_name","mystream",">",0)

>>> rc.xack("mystream","counsumer_name","1527849629172-0")

>>> rc.xgroup("mystream","counsumer_group1","$")

>>> rc.xgroup("mystream","counsumer_group2","0-0")

```



## License & Authors

Copyright (c) 2018 Max Hua

MIT (See docs/License.txt file)

The license should be the same as redis-py (https://github.com/andymccurdy/redis-py)
