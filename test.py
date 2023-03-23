from MapReduceFramework import *

clearInter()
Map('data/clicks','date')
Reduce('date','data/total_clicks')
clearInter()
Map('data/clicks','user_id','data/users','country','LT')
Reduce('user_id','data/filtered_clicks')
clearInter()