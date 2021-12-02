import numpy as np
import pandas as pd
import re
import os
from datetime import date
from datetime import datetime,timedelta

def dataextract(corpus,date,number = 1000,day =1):
    print('max rows will be:',number)
    x=corpus
    y=date
    a=y.split('-')
    b=datetime(int(a[0]),int(a[1]),int(a[2]))+timedelta(days = day)
    b=b.strftime("%Y-%m-%d")
    os.system("snscrape --jsonl --progress --max-results "+str(number)+" --since "+y+" twitter-search \""+x+" until:"+b+"\" > text-query-tweets.json")
    df = pd.read_json("text-query-tweets.json",lines=True)
    outside_list=[]
    date=[]
    l2=[] 
    for j in range(len(df)):
        if df.lang[j]=='en':
            str0=df.renderedContent[j]
            str1=' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",str0).split())
            inside_list = []
            inside_list.append(str1)
            l2.append(inside_list)
            date.append(df.date[j])
    l2=np.array(l2)
    df2 = pd.DataFrame((l2))
    x=df2.values
    outside_list.append(x)
    outside_list=np.array(outside_list)
    outside_list=np.squeeze(outside_list)
    datasetB=pd.DataFrame([outside_list,date])
    dataset=datasetB.T
    print('-'*50)
    dataset.columns = ['corpus','date']
    dataset.sort_values(by ='date').reset_index(drop=True, inplace=True)
    dataset = dataset.sort_values(by ='date').reset_index(drop=True)
    dataset['time'] = dataset['date'].dt.time
    dataset['date'] = dataset['date'].dt.date
    print('done!!!,',str(corpus),'.csv has been created')
    print('-'*50)
    dataset.to_csv(str(corpus)+'.csv',index=False)
    return dataset