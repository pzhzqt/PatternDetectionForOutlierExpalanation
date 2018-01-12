#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 16:31:31 2017

@author: sushmitasinha
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from PatternStore import addPattern
import pandas as pd 
import numpy as np
import psycopg2
import seaborn as sns
# import scipy.stats as stats
#
# from scipy.stats import chi2
# from scipy.stats import chisquare
#
# import matplotlib.pyplot as plt
# ##import re
#import sklearn as sk
#import sklearn.tree as tree

from sklearn.cluster import KMeans

df = None

'''
def formDictionary(curs, dictFixed):
    
    for row in curs:
        fixed = row[0]
        variable = row[1]
        mean = row[2]
        stdev = row[3]
        
        if fixed not in dictFixed:
            dictFixed[fixed] = {}
       
        dictFixed[fixed][variable] = float(stdev/mean)
'''

def formDictionary(curs, dictFixed, fixed, variable):
    
    for row in curs:
        
        agg = row[0]        #ALWAYS the 0th index value

        
        f = ""
        if(len(fixed) > 1):
            i = 1
            while(i <= len(fixed)):
                f = f + ":" + str(row[i] ) 
                i = i + 1
        else:
            f = row[1]
            
        v = ""
        if(len(variable) > 1):
            i = len(fixed) + 1
            while(i <= len(fixed) + len(variable)):
                v = v + ":" + str(row[i] ) 
                i = i + 1
        else:
            v = row[len(fixed) + 1]
        
        
        if f not in dictFixed:
            dictFixed[f] = {}
       
        dictFixed[f][v] = float(agg)
        
        #print(dictFixed)
'''        
def formDictionary2(curs, dictFixed):
    
    for row in curs:
        fixed = row[0]
        agg = row[1]
        
        if fixed not in dictFixed:
            dictFixed[fixed] = {}
       
        dictFixed[fixed] = float(agg)

def formQuery(fixed, variable, value, tableName):
    
    query = "SELECT " + fixed + ", " + variable + ", avg(" + value + "), stddev(" + value + ") FROM " + tableName + " where ticker in ('AAPL', 'MSFT', 'A')" +\
            " GROUP BY " + fixed + ", " + variable + " ORDER BY " + variable
    return query

'''

def formQuery(fixed, variable, value, tableName):
    
    vStr = ','.join(map(str,variable))
    fStr = ','.join(map(str,fixed))
    
    query = "SELECT stddev_pop(" + value + ")/ avg(" + value +")," + fStr + ", " + vStr + "  FROM " + tableName  +\
            " GROUP BY " + fStr + ", " + vStr + " ORDER BY " + vStr
    
    #print('Query::', query)

    return query
'''
def formQuery2(fixed, value, tableName):
    
    query = "SELECT " + fixed + ", stddev_pop(" + value + ")/ avg(" + value + ") FROM " + tableName +\
            " GROUP BY " + fixed  + " ORDER BY " + fixed
    return query
'''
def Cluster(dimensions, values, tablename, conn):
    global df
    df = pd.read_sql_query('select * from '+tablename, con=conn)

    df.select_dtypes(include=[np.number]).isnull().sum()
    df.replace('n/a', np.nan, inplace=True)
    df.fillna(0.0, inplace=True)
    df.fillna(0, inplace=True)
    return heatMap(dimensions, values)

def findConstants(dictFixed, fixed, variable, value):
    
    Cat_falseCount = 0
    Cat_trueCount = 0

    for fixedVar, plotData in dictFixed.items():
        trueCount = 0
        falseCount = 0
        for key in plotData:
            
            if (plotData[key] < .15 and plotData[key]!=0):
                trueCount = trueCount + 1
                #addPattern(fixed, fixedVar, variable, plotData[key], 'stddev', value, 'constant', plotData[key] )
                addPattern(fixed, fixedVar, variable, 'stddev1', value, 'constant', plotData[key] )

            else:
                falseCount = falseCount + 1

        if(falseCount == 0 or (trueCount/(falseCount+trueCount) > 0.75)) :

            Cat_trueCount = Cat_trueCount + 1
            #addPattern(fixed, fixedVar, variable, "none", 'stddev', value, 'constant', trueCount * 100 /(falseCount+trueCount)  )
            #addPattern(fixed, fixedVar, variable, 'stddev1', value, 'constant', trueCount * 100 /(falseCount+trueCount)  )

        else:
            Cat_falseCount = Cat_falseCount + 1

    if (Cat_falseCount == 0 or (Cat_trueCount/(Cat_trueCount+Cat_falseCount) >  0.75)):
        #addPattern(fixed, "none", variable, "none", 'stddev', value, "constant", (Cat_trueCount * 100 / (Cat_trueCount+Cat_falseCount)))
        addPattern(fixed, "none", variable, 'stddev1', value, 'constant', (Cat_trueCount * 100 / (Cat_trueCount+Cat_falseCount)))

            

'''
def findConstants2(dictFixed, fixed, value):

    Cat_falseCount = 0
    Cat_trueCount = 0

    for fixedVar, stddeviation in dictFixed.items():
        
        if(stddeviation < 0.15) :
            Cat_trueCount = Cat_trueCount + 1
            #addPattern(fixed, fixedVar, "none", "none", 'stddev', value, 'constant', stddeviation )
            addPattern(fixed, fixedVar, "none", 'stddev', value, 'constant', stddeviation )

        else:
            Cat_falseCount = Cat_falseCount + 1

    if (Cat_falseCount == 0 or (Cat_trueCount/(Cat_falseCount+Cat_trueCount) >  0.75)):
        #addPattern(fixed, "none", "none", "none", 'stddev', value, "constant", (Cat_trueCount * 100 / (Cat_trueCount+Cat_falseCount)))
        addPattern(fixed, "none", "none", 'stddev', value, 'constant', (Cat_trueCount * 100 / (Cat_trueCount+Cat_falseCount)))
'''
def correlation(dataset, thresholdpos, thresholdneg):
    col_corr = set()  # Set of all the names of columns

    corr_matrix = dataset.corr()
    for i in range(len(corr_matrix.columns)):
        for j in range(i):

            if (corr_matrix.iloc[i, j] >= thresholdpos or corr_matrix.iloc[
                i, j] <= thresholdneg):
                colname = corr_matrix.columns[i]  # getting the name of column
                colname2 = corr_matrix.columns[j]  # getting the name of column

                # statistic , pvalue = stats.chisquare( df[colname], df[colname2])
                # if(pvalue < 0.2):
                col_corr.add(colname)
                col_corr.add(colname2)

    return col_corr

def heatMap(dimension, value):
    thresholdpos = 0.7
    thresholdneg = -0.5
    df_cluster = df[dimension + value ]
    sns.heatmap (df_cluster.corr())
    df_cluster = df[dimension + value ]
    sns.heatmap (df_cluster.corr())
    cluster = KMeans(n_clusters=10)
    cluster.fit(df_cluster)
    df_cluster['clusters'] = cluster.labels_
    df_cluster.groupby('clusters').mean()
    df_heatmap = df_cluster.corr();

    col_corrHeat_dim = set()  # Set of all the names of columns
    col_corrHeat_val = set()  # Set of all the names of columns

    corr_matrix = df_heatmap.corr()
    for i in range(len(corr_matrix.columns)):
        for j in range(i):

            if (corr_matrix.iloc[i, j] >= thresholdpos or corr_matrix.iloc[
                i, j] <= thresholdneg):
                colname = corr_matrix.columns[i]  # getting the name of column
                colname2 = corr_matrix.columns[j]  # getting the name of column
                if(colname in dimension):
                    col_corrHeat_dim.add(colname)
                elif (colname in value):
                    col_corrHeat_val.add(colname)
                if(colname2 in dimension):
                    col_corrHeat_dim.add(colname2)
                elif (colname2 in value):
                    col_corrHeat_val.add(colname2)
    return list(col_corrHeat_dim), list(col_corrHeat_val)


conn = psycopg2.connect(dbname='postgres', user='antiprov',
                                    host='127.0.0.1', password='test')
df = pd.read_sql_query('select * from publication', con=conn)
#col = ['ticker', 'date', 'month', 'day', 'year','open', 'high', 'low', 'close', 'volume', 'ex-dividend', 'split_ratio', 'adj_open','adj_high', 'adj_low', 'adj_close', 'adj_volume']
#df  = pd.read_csv('/Users/sushmitasinha/Downloads/data55.csv', names=col) 

df.select_dtypes(include=[np.number]).isnull().sum()
df.replace('n/a', np.nan, inplace=True)
df.fillna(0.0, inplace=True)
df.fillna(0, inplace=True)



'''
list_corr = value_col + dimention_col
df_cluster = df[list_corr]
# sns.heatmap (df_cluster.corr())
cluster = KMeans(n_clusters=5)
cluster.fit(df_cluster)
df_cluster['clusters'] = cluster.labels_
df_cluster.groupby('clusters').mean()
df_cluster_corr = df_cluster.corr()
df_heatmap = df_cluster.corr();
print(df_cluster_hm)
'''
'''
#  fixed/value/categoricals
fixed = 'grade'
value = 'int_rate'
category =  'addr_state'
categoryvalues = list(set(df.addr_state))


#CategotyResult = findCatCorrelation3(fixed,value,category, categoryvalues)

fixed = 'grade'
value = 'int_rate'
category =  'addr_state'
categoryvalues = list(set(df.addr_state))


CategotyResult = findCatCorrelation2(value,category, categoryvalues)



  '''

