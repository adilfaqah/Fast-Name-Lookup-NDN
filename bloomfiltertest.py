'''
Created on Nov 27, 2016

@author: Adil Faqah

Counting Bloom filter implementation
'''

from bitarray import bitarray

import mmh3

class BloomFilter:
    def __init__(self, size, hash_count):
        self.size = size
        self.hash_count = hash_count
        self.bucket_array = [0 for k in range(size)]
        
    def add(self, string):
        for seed in xrange(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            if self.bucket_array[result] == 15:
                print "OVERFLOW!"
            self.bucket_array[result] += 1
            
    def remove(self, string):
        for seed in xrange(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            self.bucket_array[result] -= 1
            
    def lookup(self, string):
        for seed in xrange(self.hash_count):
            result = mmh3.hash(string, seed) % self.size
            if self.bucket_array[result] == 0:
                return "Nope"
        return "Probably"
