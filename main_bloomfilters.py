'''
Created on Nov 28, 2016

@author: Adil Faqah

NDN name look up using Bloom filters
'''
import time
import os
import cPickle as pickle
from bloomfiltertest import BloomFilter

def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]
    
class NDN_DataStruct:
    def __init__(self, size):
        self.size = size
        self.kount = 0
        self.contents = 0
        self.my_table = {}
        self.cs_bf = BloomFilter(300000, 4)
        self.pit_bf = BloomFilter(300000, 4)
        self.fib_bf = BloomFilter(300000, 4)
        
    def get_size(self):
        print self.kount, self.contents
        
    def dump_table(self):
        for host in self.my_table:
            with open(os.getcwd() + '\\mydata\\' + host + ".p", 'w+') as fp:
                #fp.write(self.my_table[host])
                pickle.dump(self.my_table[host], fp)
            #break
        self.my_table = {}
            
    def load_hostdata(self, hostname):
        dat = {}
        try:
            with open(os.getcwd() + '\\mydata\\' + hostname + ".p", 'r') as fp:
                dat = pickle.load(fp)
            return dat
        except:
            return 0
    
    def save_hostdata(self, hostname, hostdata):
        with open(os.getcwd() + '\\mydata\\' + hostname + ".p", 'w') as fp:
            pickle.dump(hostdata, fp)
    
    def search_upstream(self, lookfor, requesting_iface):
        host = lookfor.split("/", 1)[0]
        content = lookfor.split("/", 1)[1]
        #print ""
        #print " -- Incoming Interest packet!"
        
        ret = self.cs_bf.lookup(lookfor)
        #print " Is the content there?", ret
        
        if ret == "Nope":
            #print " No cached data found for this content"
            
            ret = self.pit_bf.lookup(lookfor)
            #print " Is there a PIT entry for this content?", ret
            
            if ret == "Nope":
                #print " No PIT entry found!"
                
                ret = self.fib_bf.lookup(host)
                #print " Is there a FIB entry for this host?", host, ret
                
                if ret == "Nope":
                    #print " No FIB entry found...Dropping Interest packet!"
                    return
                
                elif ret == "Probably":
                    hostdata_dict = self.load_hostdata(host)
                    if hostdata_dict == 0:
                        print " Confirmed: No PIT entry found...Dropping Interest packet!"
                    else:
                        #print " Created PIT entry for this content!"
                        self.pit_bf.add(lookfor)
                        
                        my_dict = {}
                        my_dict["requesting_ifaces"] = [requesting_iface]
                        my_dict["DATA"] = None
                        my_dict["TLA"] = time.time()

                        hostdata_dict[content] = my_dict
                        #print hostdata_dict[content]
                        self.save_hostdata(host, hostdata_dict)
                        #print " FIB entry found...Forwarding Interest packet!"     
                
            elif ret == "Probably":
                hostdata_dict = self.load_hostdata(host)
                if hostdata_dict == 0:
                    print " Error:  No route to host...Dropping packet!"
                else:
                    print " Added requesting interface to PIT entry"
                    hostdata_dict[content]["requesting_ifaces"].append(requesting_iface)
                    self.save_hostdata(host, hostdata_dict)
            
            
        elif ret == "Probably":
            hostdata_dict = self.load_hostdata(host)
            if hostdata_dict == 0:
                print " Confirmed:  No cached data found for this content!"
            else:
                #print "Data ", hostdata_dict[content]["DATA"]
                return hostdata_dict[content]["DATA"]
            
       
    
    def search_downstream(self, lookfor, data):
        host = lookfor.split("/", 1)[0]
        content = lookfor.split("/", 1)[1]
        print ""
        print " -- Incoming Data packet!"
        if host in self.my_table:
            print " Found the host!"
            print self.my_table[host]
            
            if content in self.my_table[host]:
                print " Found PIT entry for this content!"
                print self.my_table[host][content]
                self.my_table[host][content]["TLA"] = time.time()
                
                print " Cached data from this packet"
                self.cs_bf.add(lookfor)
                
                self.my_table[host][content]["DATA"] = data
                
                print "Forwarding data to: ", self.my_table[host][content]["requesting_ifaces"]
                self.my_table[host][content]["requesting_ifaces"] = []
            else:
                print " No PIT entry found! Dropping data packet"
        else:
            print "Not in table!"
        
            
    def add(self, line):
        if self.kount == self.size:
            print "Table full!"
            return
        if "http://" not in line and "https://" not in line and "ftp://" not in line:
            #print "not http or https: ", line
            return
        host = line.replace("\n","").split(",")[1].replace("http://","").replace("https://","").replace("ftp://","").split("/", 1)[0]
        if len(host) == 0:
            print "host is 0:", line
            return
        if host in self.my_table:
            try:
                content = line.replace("\n","").split(",")[1].replace("http://","").replace("https://","").replace("ftp://","").split("/", 1)[1]
                if len(content) == 0:
                    return
                
                my_dict = {}
                my_dict["requesting_ifaces"] = list()
                #my_dict["forwarding_ifaces"] = [0, 1, 2]
                my_dict["DATA"] = 1
                my_dict["TLA"] = time.time()
                #print my_dict

                self.my_table[host][content] = my_dict
                x = line.replace("\n","").split(",")[1].replace("http://","").replace("https://","").replace("ftp://","")
                #print x
                self.cs_bf.add(x)
                
                self.contents += 1
                #print host
            except:
                print line
                print host
                print "--"
        else:
            if host == "ajpheart.physiology.org":
                
                print host
            self.fib_bf.add(host)
            self.my_table[host] = {}
            self.my_table[host]["forwarding_ifaces"] = [0, 1, 2]
            #print self.my_table
            self.kount += 1
            
my_ds = NDN_DataStruct(1000000)
# 
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
my_ds.dump_table()
print "Gone to sleep"
time.sleep(5)

maxtime = 0
mintime = 1000000000
average = 0
summ = 0
kount = 1

with open(os.getcwd() + "\\testcases.txt", 'r') as fp:
    for lines in fp:
        lookfor = lines.replace("\n","")
        #lookfor = "xajpheart.physiology.org/cgi/doi/10.1152/ajpheart.00665.2010"
        #print lookfor

        s_time = time.clock()
        my_ds.search_upstream(lookfor, kount)
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
        
#         if kount == 10:
#             break
        
print "maxtime = ", maxtime*1000000, ",   min time = ", mintime*1000000, ",   avg time = ", average*1000000, kount
