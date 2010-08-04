# Copyright (C) 2009, Hyves (Startphone Ltd.)
#
# This module is part of the Concurrence Framework and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php

#this is a pretty straightforward pure python implementation of
#libketama (a consistent hashing implementation used by many memcached clients)
#    http://www.last.fm/user/RJ/journal/2007/04/10/rz_libketama_-_a_consistent_hashing_algo_for_memcache_clients
#    svn://svn.audioscrobbler.net/misc/ketama/
#a good explanation of constent hashing can be found here:
#   http://www.spiteful.com/2008/03/17/programmers-toolbox-part-3-consistent-hashing/
#
#the test server_list below was taken from the libketama distribution and 1 million keys were mapped with
#both this implementation and the libketama to test this implementations compatibility

import math
import hashlib
import bisect
import unittest

def key_to_digest(key):
    return hashlib.md5(key).hexdigest()

def point_from_hex(s):
    return long(s[6:8] + s[4:6] + s[2:4] + s[0:2], 16)

def hashi(key):
    return point_from_hex(key_to_digest(key)[0:8])

def get_server(key, continuum):
    """maps given key to a server in the continuum"""
    point = hashi(str(key))
    i = bisect.bisect_right(continuum, (point, ()))
    if i < len(continuum):
        return continuum[i][1]
    else:
        return continuum[0][1]

def build_continuum(servers):
    """builds up the 'continuum' from the given list of servers.
    each item in the list is a tuple ((ip_addr, port), weight).
    where ip_addr is an ip-address as a string, e.g. '127.0.0.1'.
    port is the ip-port as an integer and weight is the relative weight
    of this server as an integer.
    """
    continuum = {}
    memory = sum([s[1] for s in servers]) #total weight of servers (a.k.a. memory)
    server_count = len(servers)
    for server in servers:
        pct = float(server[1]) / memory #pct of memory of this server
        ks = int(math.floor(pct * 40.0 * server_count))
        for k in range(ks):
            # max 40 hashes, 4 numbers per hash = max 160 points per server */
            ss = "%s:%s-%d" % (server[0][0], server[0][1], k)
            digest = key_to_digest(ss)
            for h in range(4):
                point = point_from_hex(digest[h * 8: h * 8 + 8])
                if not point in continuum:
                    continuum[point] = server[0]
                else:
                    assert False, "point collission while building continuum"
    return sorted(continuum.items())


class TestKetama(unittest.TestCase):
    test_servers = [(('10.0.1.1', 11211), 600),
                    (('10.0.1.2', 11211), 300),
                    (('10.0.1.3', 11211), 200),
                    (('10.0.1.4', 11211), 350),
                    (('10.0.1.5', 11211), 1000),
                    (('10.0.1.6', 11211), 800),
                    (('10.0.1.7', 11211), 950),
                    (('10.0.1.8', 11211), 100)]

    def testKetama(self):

        continuum = build_continuum(self.test_servers)

        self.assertEquals((3769287096, ('10.0.1.7', 11211)), (hashi('12936'), get_server('12936', continuum)))
        self.assertEquals((435768809, ('10.0.1.5', 11211)), (hashi('27804'), get_server('27804', continuum)))
        self.assertEquals((1996655674, ('10.0.1.2', 11211)), (hashi('37045'), get_server('37045', continuum)))
        self.assertEquals((2954822664, ('10.0.1.1', 11211)), (hashi('50829'), get_server('50829', continuum)))
        self.assertEquals((1423001712, ('10.0.1.6', 11211)), (hashi('65422'), get_server('65422', continuum)))
        self.assertEquals((3809055594, ('10.0.1.6', 11211)), (hashi('74912'), get_server('74912', continuum)))

if __name__ == '__main__':
    unittest.main()

