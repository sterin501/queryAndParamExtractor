#!/usr/bin/python3
import json,re,os,shutil
import ast
from datetime import datetime as dt
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-t", "--ticket", help="provide ticket number")
parser.add_argument("-f", "--filename", help="filename")
parser.add_argument("-l", "--links", help="links by search")
args = parser.parse_args()

# command queries from browser
skipQuery=["ops-manager-server","embedded-session","custom.dataCleanRetention","db.clearQueryCaches","db.schema.nodeTypeProperties",\
"data:COLLECT(label)[..1000]}","dbms.procedures()","dbms.functions()","bloom.fetchPerspectiveSha","db.constraints"]



def folderManage():               # manage folder
    if os.path.exists("q"):
        shutil.rmtree("q")
    if os.path.exists("param"):
        shutil.rmtree("param")
    os.mkdir("q")
    os.mkdir("param")


def convertStingTojson(st,count):
    data={}
    try:
       #print (st)
       pattern = r"'([^']*)'"
       st1 = re.sub(pattern, replace_colon, st)    # replace all : in the parameter
       pattern = r'(\w+):'
       st2 = re.sub(pattern, r'"\1":', st1)           # replace first part of the json
       st3=re.sub(r"'", '"', st2)                     # replace all ' to " for proper json

       st4=st3.replace("<null>","null")               # null objects

       st5=st4.replace("}{","},{")                     # nested dict
       st6=st5.replace("~Colon~",":")                  # replace back to :
       data = json.loads(st6)
    except Exception as e:
        print (f"{e} at query line {count}   . It is returning as string instead a json . Edit before running ")
        #print(st)
        #print (st5)
        return (st5)
    return (data)

def replace_colon(match):                                 # to manage : in the parameter part
    if  isJson (match.group(0)):
        return (match.group(0))
    return match.group(0).replace(':', '~Colon~')

def isJson(st):
    try:
       st=st[1:-1]
       #print (type(st))
       data = json.loads(st)
       return True
    except Exception as e:
        return False

def extractQueryParam(count,qpart):
    objbydash=qpart.split(" - ")                              # split the query part by -
    paramindex=3
    realquery=objbydash[paramindex-1]
    if re.match(r'^\{.*\}$', objbydash[paramindex]):                # Get parameter part
        param=convertStingTojson(objbydash[paramindex],count)
    else:
            while paramindex < len(objbydash):                         # to get - in the query
                paramindex+=1
                realquery=realquery+" - "+objbydash[paramindex-1]
                if re.match(r'^\{.*\}$', objbydash[paramindex]):
                             param=convertStingTojson(objbydash[paramindex],count)
                             break

    return ({"query":realquery,"param":param,"linecount":count})

def checkinSkip(line):                             # skipping command quries
    for kk in skipQuery:
        if   kk in line:
            return True
    return False

def readLogs(filename):
    listoflogs = []
    skippedLines=[]
    skippedQuriesatLine=[]
    print("creating json for file {}".format(filename))
    count=0
    listofJson=[]
    with open(filename, "r") as file:
        # Read the entire file content into the string
        logfull = file.read()
    logfullList=logfull.split("\n")
    date_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3}\+\d{4}'  # Regex to check starting of the line
    linetosend=""
    lc=(len(logfullList))
    print (f" Lines to process {lc}")
    logbyline=list(logfullList)
    for line in logbyline:
            try:
                count = count + 1
                if (lc==count+1) or (re.match(date_pattern,logfullList[count])) :
                     linetosend=linetosend+"\n"+line

                else:
                    linetosend=linetosend+"\n"+line
                    continue                                         # if the line is not start with date , then append to old one

                objbyTab=linetosend.split("\t")                      # split by tab to get each field
                linetosend=""          ## Resetting
                toprocess=objbyTab[-1]                                # Last part is query
                #   print (f" length    is   {len(objbyTab)}")
                if len(objbyTab)>7:                                    # To mange tab in the query part
                    toprocess=""
                    for element in objbyTab[6:]:
                              toprocess =toprocess+" "+element

                if (toprocess.startswith("system") or  checkinSkip(toprocess)):   # skip system database query and common query from browser
                    skippedQuriesatLine.append(count)
                    continue

                listofJson.append(extractQueryParam(count,toprocess))              # extract param
                # for kk in  (objbyspace):
                #     print (kk)
                # print ("*"*50)


                #print (objbyspace)
            except Exception as e:
                print (f"Exception occured {e}")
                skippedLines.append(count)
                continue
    print ("\nprocessed 100 %")
    if (len(skippedQuriesatLine) > 100):
      savefile("skippingline.txt",str(skippedLines))
      savefile("skippingQuery.txt",str(skippedQuriesatLine))
    else:
      print (f"\nSkipping lines due to parasing error {skippedLines}   count {len(skippedLines)}")
      print (f"\nSkiped Query Lines {skippedQuriesatLine} count {len(skippedQuriesatLine)} ")
    return (listofJson)

def  savefile(filename,string):
    with open(filename, "w") as file:
       file.write(string)


if __name__ == '__main__':
    folderManage()
    if args.filename:
       filename=(args.filename)
    else:
       filename = 'query.log'   # argment logic
    print ("Reading queries")
    listofq=readLogs(filename)   # create the list
    count=0
    print ("\n\nSaving  queries")
    linetoQueryMapping={}

    for kk in listofq:                    # creating the query and params from list
        count=count+1
        qfile_path="q/"+str(count)+".cypher"        # saving cypher
        with open(qfile_path, "w") as file:
           file.write(kk['query'])
        jfile_path="param/"+str(count)+".json"       # saving params
        with open(jfile_path, "w") as file:
               json.dump(kk['param'], file)
        linetoQueryMapping[count]=kk['linecount']
    print (f"\n{count}  saved")
    savefile("linetoQueryMapping.json",json.dumps(linetoQueryMapping))
    if len(linetoQueryMapping)>100:
        print ("Due to large size mapping details are saved in text file")
    else:
      print (f"\n Line to Query     {linetoQueryMapping}")                  # line to query details
