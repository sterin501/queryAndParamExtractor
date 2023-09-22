# queryAndParamExtractor

This is used to extract the queries and parameters from the query.log. After extracting the query, it can be run on the Neo4j server. .
Tab ("\t")  is used to split the query. However, it might not work with the file that was created  by copy-pasting

Usage 

To install required python libary files 
pip -r requirements.txt


To extract quries 
./queryExtractor.py -f <query.log> 

This will create two folders   q and params 

To run every query to neo4j server 

./runfromqfolder.py 

