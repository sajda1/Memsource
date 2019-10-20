import os
import json
from operator import itemgetter
import itertools

# CHANGE PATH
os.chdir('C:/Users/Martin/Desktop/Assignment - Big Data Engineer/skeleton/src/main/resources/')

events = []
for line in open('kafka-messages.jsonline', 'r'):
    events.append(json.loads(line))

# remove events with levels = null and op != "c" 
eventsFiltered = []
for i in events:
    if i['after'] != None and i['op'] == 'c' :
        innerJson = json.loads(i['after'])
        if innerJson['levels'] != None:
            eventsFiltered.append(i)

# get the match 
taskConformation = []
for event in eventsFiltered:
    innerJson = json.loads(event['after'])
    
    taskId = innerJson['taskId']
    levels = innerJson['levels'][0]
    confirmedLevel = innerJson['tUnits'][0]['confirmedLevel']
    
    if levels == confirmedLevel:
        taskConformation.append ({'taskId':taskId ,'conformation': 1} )
    else:
        taskConformation.append ({'taskId':taskId ,'conformation': 0 })

# group by 
taskConformation.sort(key=itemgetter('taskId'))
taskCount = [] 
for key, group in itertools.groupby(taskConformation, lambda item: item['taskId']):
    taskCount.append ( {key: sum([item['conformation'] for item in group]) })
    
print(taskCount)