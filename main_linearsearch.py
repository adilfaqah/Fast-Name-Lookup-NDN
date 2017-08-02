'''
Created on Nov 28, 2016

@author: Adil Faqah

NDN name look up using linear search and linked list
'''

import time
import os
import cPickle as pickle
from matplotlib.mathtext import DELTA

def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]
    
class NDN_DataStruct:
    def __init__(self, size):
        self.size = size
        self.kount = 0
        self.contents = 0
        self.FIB = []
        self.PIT = []
        self.CS = []
        
    def get_table(self):
        return self.my_table
        
    def dump_table(self):
        with open('data.p', 'w') as fp:
            pickle.dump(self.my_table, fp)
            
    def load_table(self):
        with open('data.p', 'r') as fp:
            self.my_table = pickle.load(fp)
        
    def search_upstream(self, lookfor, requesting_iface):
        #print ""
        #print " -- Incoming Interest packet!"
        host = lookfor.split("/", 1)[0]
        #print self.FIB
        #print host
        for kontent in self.CS:
            #print kontent
            if kontent[0] == lookfor:
                #print "cS match"
                #print self.CS[lookfor]["DATA"]
                return
        #print "Content not found..looking in PIT"
        for ppit in self.PIT:
            if ppit[0] == lookfor:
                #print "PIT entry found...searching for this iface"
                for intface in ppit[1]:
                    if intface == requesting_iface:
                        #print "already there"
                        return

                    print "adding new iface"
                    ppit[1].append(requesting_iface)
                    return
                
        #print "No pit entery found"
        #print "searching in FIB"
                
        for ffib in self.FIB:
            if ffib[0] == host:
                #print "Found in FIB!"
                #print "Adding PIT entry"
                ppit[1].append(requesting_iface)
                return
        #print "dropping packet"
        return
                
    
    def search_downstream(self, lookfor, data):
        host = lookfor.split("/", 1)[0]
        content = lookfor.split("/", 1)[1]
        #print ""
        print " -- Incoming Data packet!"
        if host in self.my_table:
            print " Found the host!"
            print self.my_table[host]
            
            if content in self.my_table[host]:
                print " Found PIT entry for this content!"
                print self.my_table[host][content]
                self.my_table[host][content]["TLA"] = time.time()
                
                print " Cached data from this packet"
                self.my_table[host][content]["DATA"] = data
                
                print "Forwarding data to: ", self.my_table[host][content]["requesting_ifaces"]
                self.my_table[host][content]["requesting_ifaces"] = []
            else:
                print " No PIT entry found! Dropping data packet"
        else:
            print "Not in table!"
        
    def get_size(self):
        print self.kount, self.contents
            
    def add(self, line):
        if self.kount == self.size:
            print "Table full!"
            return
        if "http://" not in line and "https://" not in line and "ftp://" not in line:
            return
        complete = line.replace("\n","").split(",")[1].replace("http://","").replace("https://","").replace("ftp://","")
        host = line.replace("\n","").split(",")[1].replace("http://","").replace("https://","").replace("ftp://","").split("/", 1)[0]
        if len(host) == 0:
            print "host is 0:", line
            return
        if not complete in self.CS:
            try:
                
                #my_dict["TLA"] = time.time()
                new = [complete+'x', 0]
                self.CS.append(new)
                #print self.CS
                #self.CS[complete] = my_dict
                #print self.CS
                new = [complete, []]
                self.PIT.append(new)
                #self.PIT[complete] = [0]
                
                self.contents += 1
                #self.FIB[host] = {}
                #self.FIB[host]["forwarding_ifaces"] = [0, 1, 2]
            except:
                print line
                print host
                print "--"
        if not host in self.FIB:
            new = [host, [0, 1, 2]]
            self.FIB.append(new)
            #print self.FIB

            #self.FIB[host] = {}
            #self.FIB[host]["forwarding_ifaces"] = [0, 1, 2]
            self.kount += 1
            
my_ds = NDN_DataStruct(1000000)

s_time = time.clock()
kount = 0
dict_directories =  get_immediate_subdirectories((os.getcwd() + "\\doi-urls"))
for directory in dict_directories:
    print "Importing directory:", directory
    dataset_file = open(os.getcwd() + "\\doi-urls\\" + directory + "\\" + directory.split(".")[0] + ".csv", "r")
    for line in dataset_file:
        if kount == 10242:
            break
        my_ds.add(line)
        kount += 1
    dataset_file.close()
    print "Load 1: ", (time.clock() - s_time), "s"
    print "Importing complete..."
print kount
my_ds.get_size()
    
maxtime = 0
mintime = 1000000000
average = 0
summ = 0
kount = 1

with open(os.getcwd() + "\\testcases.txt", 'r') as fp:
    for lines in fp:
        lookfor = lines.replace("\n","")#+"lool"+str(kount)
        #lookfor = "xajpheart.physiology.org/cgi/doi/10.1152/ajpheart.00665.2010"
        #print lookfor
  
        s_time = time.clock()
        my_ds.search_upstream(lookfor, 0)
        e_time = time.clock()
        #print "Search time: ", (e_time - s_time)*1000000, "us"
        delta = e_time - s_time
        if delta > maxtime:
            maxtime = delta
        if delta < mintime:
            mintime = delta
        summ += delta 
        average = summ/kount
          
        kount += 1
           
#         if kount == 2:
#             break
          
print "maxtime = ", maxtime*1000000, ",   mine time = ", mintime*1000000, ",   avg time = ", average*1000000, kount
 
time.sleep(10)
