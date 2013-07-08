#!/usr/bin/env python

import sys
import random
import Queue

# probability to have "cheap" alternate location
prob_cheap   = 0.5
# the range to search empty slot for cheap keys
search_range = 4
# how many slots in each bucket
slots_per_bucket = 4

MAX_NCHECKED  = 500

hash_power    = 15



def key_type():
    if (random.random() < prob_cheap):
        return 1
    else:
        return 2


class CuckooHashTable(object):

    def __init__(self, num_buckets):
        self.num_keys = 0
        self.table = []
        self.num_buckets = num_buckets
        for i in range(num_buckets):
            self.table.append([])
            for j in range(slots_per_bucket):
                self.table[i].append(None)

    def is_empty(self, i):
        for j in range(slots_per_bucket):
            if self.table[i][j] == None:
                return (True, j)
        return (False, None)


    def search_path(self,  start_buckets):
        q = Queue.Queue()
        for i in start_buckets:
            for j in range(slots_per_bucket):
                q.put([(i, j)])
                assert(self.table[i][j] != None)
        nchecked = 0

        while not q.empty():
            path = q.get()
            (i, j) = path[-1]
            (key_t, key) = self.table[i][j]
            if (key_t == 1):
                # try to extend cuckoo path on a cheap key
                ii = key + 1
                while (ii <= key  + search_range):
                    nchecked += 1
                    i = ii & (self.num_buckets - 1)
                    for j in range(slots_per_bucket):
                        if self.table[i][j] == None:
                            newpath = list(path)
                            newpath.append((i,j))
                            return newpath
                        (t, k) = self.table[i][j]
                        if (t == 1):
                            pass
                        else:
                            newpath = list(path)
                            newpath.append((i, j))
                            q.put(newpath)
                    ii += 1
                                         
            else:
                # try to extend cuckoo path on an expensive key
                nchecked += 1
                i1, i2 = key
                i2 = i1 + i2 - i
                for j2 in range(slots_per_bucket):
                    newpath = list(path)
                    newpath.append((i2, j2))
                    if self.table[i2][j2] == None:
                        return newpath
                    else:
                        q.put(newpath)

            if (nchecked > MAX_NCHECKED):
                return []
        return []
        

    def insert(self, key_t, key):
        #print "insert", self.num_keys, self.num_buckets*slots_per_bucket, key_t, key
        if (key_t == 1):
            # a 'cheap' key
            i = key
            
            for dist in range(search_range):
                (ret, j) = self.is_empty(i)
                if ret == True:
                    self.table[i][j] = (key_t, key)
                    self.num_keys += 1
                    return True
                i = (i + 1) & (self.num_buckets - 1)

            start_buckets = [key]

        else:
            # this is an 'expensive' key
            (i1, i2) = key
            
            # check the first bucket i1
            (ret, j1) = self.is_empty(i1)
            if ret == True:
                self.table[i1][j1] = (key_t, key)
                self.num_keys += 1
                return True

            # unluck in bucket i1, now try bucket i2
            (ret, j2) = self.is_empty(i2)
            if ret == True:
                self.table[i2][j2] = (key_t, key)
                self.num_keys += 1
                return True
            
            start_buckets = [i1, i2]

        
        path = self.search_path(start_buckets)
        if path == []:
            return False

        
        cur = (key_t, key)

        for (i, j) in path:
            next = self.table[i][j]
            self.table[i][j] = cur
            cur  = next
        assert(cur == None)
        self.num_keys += 1
        return True

    def load_factor(self):
        return 1.0 * self.num_keys / self.num_buckets / slots_per_bucket

    def num_key_type(self):
        n1, n2 = 0, 0
        for bucket in self.table:
            for slot in bucket:
                if slot == None: continue
                (key_t, key) = slot
                if key_t == 1: n1 += 1
                elif key_t == 2: n2 += 1
        return (n1 , n2)


def main():
    # global parameters
    num_buckets      = 1 << hash_power
    slots_per_bucket = 4
    table = CuckooHashTable(num_buckets)

    flag = True
    while flag:
        key_t = key_type()
        if (key_t == 1):
            key = random.randrange(num_buckets)
            flag = table.insert(key_t, key)
        else:
            i1 = random.randrange(num_buckets)
            i2 = random.randrange(num_buckets)
            flag = table.insert(key_t, (i1, i2))

    print "prob to have cheap keys:", prob_cheap
    print "search range for cheap keys:", search_range
    print "total load factor:", table.load_factor()
    print "(# cheap keys, # exp keys) in table:", table.num_key_type()

if __name__ == "__main__":
    main()
