#!/usr/bin/env python
# coding: utf-8

# ### Worker Logic:
# ##### Take the partition. Run the simulations and out the files in S3 Output Bucket

# In[7]:


def partitionedWorker(partId,numPaths):
    from io import StringIO
    import pandas as pd
    import boto3
    data = pd.read_csv('https://sg-test-bbl-eks.s3.amazonaws.com/input/Portfolio.csv')
    df = pd.DataFrame(data, columns= ['PartId',' ID','Name','Balance'])
    print(df.dtypes)
    #print(df.head())
    mydf=pd.DataFrame(columns = ['PartId', 'ID', 'Name','SimulatedVal'])
    mypart=df.loc[df['PartId'] == partId]
    #print(mypart)
    
    for i in range(numPaths):
        
        for index, row in mypart.iterrows():
            #print(row['PartId'], row[' ID'],row['Name'],index)
            simData=[[row['PartId'],row[' ID'],row['Name'],i]]
            simDFOut=pd.DataFrame(simData, columns = ['PartId', 'ID','Name','SimulatedVal']) 
            mydf=mydf.append(simDFOut)
            #print(mydf.head())
    bucket = 'sg-test-bbl-eks'  # already created on S3
    csv_buffer = StringIO()
    mydf.to_csv(csv_buffer)
    session = boto3.Session(
    aws_access_key_id="AKIAWBCUGUJV7HNCSX5S",
    aws_secret_access_key="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",
    )
    
    s3_resource = session.resource('s3')
    s3_resource.Object(bucket, 'output/ '+partId+'/mydf.csv').put(Body=csv_buffer.getvalue())
    print("Succesfully done for Partition {}".format(partId))
    return mydf
 

import sys
import datetime
print ('Number of arguments:', len(sys.argv), 'arguments.')
print ('Argument List:', (sys.argv[1]))


partId=int(sys.argv[1])
numPaths=int(sys.argv[2])
print ("Start Time for Partition {} processing = ".format(partId),datetime.datetime.now())
partitionedWorker(partId,numPaths)

# from io import StringIO
# import pandas as pd
# import boto3
# mydf=pd.DataFrame(columns = ['PartId', 'ID', 'Name','SimulatedVal'])
# mypart=df.loc[df["PartId"]==partId]
# #print(mypart)

# for i in range(numPaths):
    
#     for index, row in mypart.iterrows():
#         #print(row['PartId'], row[' ID'],row['Name'],index)
#         simData=[[row['PartId'],row[' ID'],row['Name'],i]]
#         simDFOut=pd.DataFrame(simData, columns = ['PartId', 'ID','Name','SimulatedVal']) 
#         mydf=mydf.append(simDFOut)
#         #print(mydf.head())
# bucket = 'sg-test-bbl-eks'  # already created on S3
# csv_buffer = StringIO()
# mydf.to_csv(csv_buffer)
# session = boto3.Session(
# aws_access_key_id="AKIAWBCUGUJV7HNCSX5S",
# aws_secret_access_key="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",
# )

# s3_resource = session.resource('s3')
# s3_resource.Object(bucket, 'output/mydf.csv').put(Body=csv_buffer.getvalue())
# #mydf.to_csv("s3://sg-test-bbl-eks/output/{}.csv".format(partId))
# #return mydf






