#!/usr/bin/python3
from neo4j import GraphDatabase
import random,os,json
import sortedcontainers


driver = GraphDatabase.driver(uri="neo4j://localhost:7687", auth=("neo4j", "secret"))

def createlistofq():

 qs=[]
 with open("linetoQueryMapping.json", "r") as f:
          querlinemapping = json.load(f)
 files = os.listdir('q')
 sorted_files = sortedcontainers.SortedList(files, key=lambda file: int(file.replace('.cypher','')))
 for file_name in sorted_files:
    query=""
    param=""
    with open('q/'+file_name, 'r') as file:
        query=file.read()
    param_file='param/'+file_name.replace('cypher','json')
    #if os.path.exists(param_file):
    with open(param_file, "r") as f:
             param = json.load(f)
    sc=file_name.replace('.cypher','')
    qs.append({'query':query,'param':param,'name':file_name,'queryline':querlinemapping[sc]})

 return (qs)


def runq(qs):
 with driver.session() as session:
      for query in qs:
          print (f"running {query['name']}  :query.log line  <>  {query['queryline']} ")
          try :
             result = session.run(query['query'],query['param'])
          except Exception as e:
              print (f" {query['name']}  throws --> {e}")



if __name__ == '__main__':
    qs=createlistofq()
    runq(qs)
