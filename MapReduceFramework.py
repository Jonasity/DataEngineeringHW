#MapReduce Framework
import csv,time,threading
from os import listdir,remove,path,makedirs
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
      """

class MapThread (threading.Thread):
    def __init__(self, threadID, name, source, data, key):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.source = source
        self.data = data
        self.key = key

    def run(self):
        print("Starting " + self.name)
        file = open(self.source+"/"+self.data)
        read = csv.reader(file)
        index = next(read).index('{}'.format(self.key))
        out = open("inter/"+str(self.threadID)+".csv", "x")
        pairs = {}
        for row in read:
            if row[index] in pairs:
                pairs[row[index]] += 1
            else:
                pairs.update({row[index]: 1})
        for k in pairs.keys():
            out.write("%s,%s\n" % (k,pairs[k]))
        out.close
        file.close
        print("Exiting " + self.name)

def MapReduce(source,key,output):
    if not path.exists("inter"): 
        makedirs("inter")
    iterator = 0
    dataset = [f for f in listdir(source)] #Find all dataset files
    for data in dataset:
        thread = MapThread(iterator, "map-"+str(iterator), source, data, key)
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
    #Reduce
    finalpairs = {}
    interset = [f for f in listdir("inter")]
    for inter in interset:
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

    
        
def clearInter():
    if path.exists("inter"):
        files = [f for f in listdir("inter")]
        for file in files:
            remove("inter/"+file)
    return


clearInter()
MapReduce('data/clicks','date','data/clicks_per_day')