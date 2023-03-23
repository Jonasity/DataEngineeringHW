#MapReduce Framework
""" Pseudo code
Map()
    List all files in given dataset directory
    (Maybe separate each file to a different thread for the function)
    parse through files creating intermediate key value pairs
    ouput results to intermediate files

Reduce()
    Parse through all intermediate files
    Sort all intermediate pairs bto single file


Need to consider how to join
    - clicks that belong to country=LT
    - we have table of country and ID
    - map id and country?
Perhaps write a separate function called Join that will take two datasets/files as input and combine them based
on a primary key, removing any rows that don't fit, then user calls Join, and then MapReduce

Another idea, define another function in MapThread class, which instead of default taking count of objects, will take a second key to collect those records matching a key
      """
import csv,time,threading
from os import listdir,remove,path,makedirs
class MapThread (threading.Thread):
    def __init__(self, threadID, name, source, data, key, joinpair):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.source = source
        self.data = data
        self.key = key
        self.joinpair = joinpair

    def run(self):
        print("Starting " + self.name)
        file = open(self.source+"/"+self.data)
        read = csv.reader(file)
        index = next(read).index('{}'.format(self.key))
        out = open("inter/"+str(self.threadID)+".csv", "w")
        pairs = {}
        for row in read:
            if row[index] in pairs:
                pairs[row[index]] += 1
            else:
                if not self.joinpair:
                    pairs.update({row[index]: 1})
                else:
                    if row[index] in self.joinpair:
                        pairs.update({row[index]: 1})
        for k in pairs.keys():
            out.write("%s,%s\n" % (k,pairs[k]))
        out.close
        file.close
        print("Exiting " + self.name)
        return

#Map
def Map(source,key,source2="",key2="",value=""):
    if not path.exists("inter"): 
        makedirs("inter")
    joinpair = []
    if source2 != "":
        dataset2 = [f for f in listdir(source2)]
        for data in dataset2:
            file = open(source2+"/"+data)
            read = csv.reader(file)
            index = next(read).index('{}'.format(key2))
            for row in read:
                if row[index] == value:
                    joinpair.append(row[0])
    iterator = 0
    dataset = [f for f in listdir(source)] #Find all dataset files
    for data in dataset:
        thread = MapThread(iterator, "map-"+str(iterator), source, data, key, joinpair)
        thread.start() #Begin thread operations
        iterator+=1
    timeout = time.time()+30 #30 seconds till timeout
    while threading.active_count() > 1:
        if time.time() < timeout:
            time.sleep(0.1)
        else:
            print("Error occured in map threads")
            break
    print("Map Done")
    return

#Reduce
def Reduce(key,output):
    finalpairs = {}
    interset = [f for f in listdir("inter")]
    for inter in interset: #Store all inter file values to one dictionary
        file = open("inter/"+inter)
        read = csv.reader(file)
        for row in read:
            if row[0] in finalpairs:
                finalpairs[row[0]] += int(row[1])
            else:
                finalpairs.update({row[0]:int(row[1])})
        file.close
    out = open(output + ".csv", "w")
    out.write("%s,count\n" % (key))
    for k in finalpairs.keys():
        out.write("%s,%s\n" % (k,finalpairs[k]))
    out.close
    print("Output complete")
    return
    
        
def clearInter(): # clears intermediate files
    if path.exists("inter"):
        files = [f for f in listdir("inter")]
        for file in files:
            remove("inter/"+file)
    return