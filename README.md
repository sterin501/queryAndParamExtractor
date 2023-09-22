# queryAndParamExtractor

This used to extract the queries and parameters from query.log . After extracting the query , it can run to the neo4j server .
Tab is used for spliting the query.It might not work the file which created by copy pasting

Usage 

pip -r requirements.tx 

To extract quries 
./queryExtractor.py -f <query.log> 

This will create two folders   q and params 

To run every query to neo4j server 

./runfromqfolder.py 

