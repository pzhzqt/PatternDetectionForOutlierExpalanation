#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 16:05:11 2017

@author: deeptichavan
"""

import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from PatternStore import addPattern
import PatternFinder as pf

def formQuery(fixed, variable, aggFunc, value, tableName):
    
    vStr = ','.join(map(str,variable))
    fStr = ','.join(map(str,fixed))
    
    query = "SELECT " +aggFunc+"(" + value + "), " + fStr + ", " + vStr + "  FROM " + tableName  +\
            " GROUP BY " + fStr + ", " + vStr + " ORDER BY " + vStr
    
    #print('Query::', query)

    return query


def performLinearRegression(x ,y, r):
    
    lr = LinearRegression(normalize=True)
    lr.fit(x,y)
    
    slope = lr.coef_
    
    ytest = lr.predict(x)
    
    x = x.astype(float)
    y = y.astype(float)
    scoreLR = lr.score(x, y)
        
    mse = mean_squared_error(y, ytest)
    rmse = np.sqrt(mse)
        
    return float(slope), rmse, ytest, scoreLR

def plotLinearRegression(x, y, yPltLR, scoreLR, fixed):
    
    lw = 2
    fig = plt.figure()
    fig.set_size_inches(10.5, 6.5)
    #plt.xlabel('months')
    #plt.ylabel(label)
    plt.suptitle("Fixed:: " + ','.join(map(str,fixed)) +  "  Score :: " + str(scoreLR)) 
      
    plt.scatter(x,y)
    plt.plot(x, yPltLR, color='navy', linewidth=lw, label='Linear Regressor')
    
    plt.legend(loc='lower right')
    #plt.show()
    
    #pf.pdf.savefig(fig)
    
    return

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


def fitRegressionModel(dictFixed, fixed, variable, aggFunc, value):
    
    validPatterns = 0
    for fixedVar, plotData in dictFixed.items():
    
        x = []
        y = []
        count = 1
        for key in plotData:
            #x.append(key)
            x.append(count)
            count = count + 1
            y.append(plotData[key])
                        
        
        x = np.transpose(np.reshape(x,[1,len(x)]))
        y = np.transpose(np.reshape(y,[1,len(y)]))
        r = x.size
        
        slopeLR, rmseLR, yPltLR, scoreLR = performLinearRegression(x, y, r)
        
        if(slopeLR > 0 and scoreLR > 0.7):
            validPatterns = validPatterns + 1
            addPattern(fixed, fixedVar, variable, aggFunc, value, 'increasing', scoreLR)
            #plotLinearRegression(x, y, yPltLR, scoreLR, fixedVar)
            
        elif(slopeLR < 0 and scoreLR > 0.7):
            validPatterns = validPatterns + 1
            addPattern(fixed, fixedVar, variable, aggFunc, value, 'decreasing', scoreLR)
            #plotLinearRegression(x, y, yPltLR, scoreLR, fixedVar)
                  
        #plotLinearRegression(x, y, yPltLR, scoreLR, fixed)
    
    addPattern(fixed, "none", variable, aggFunc, value, "linear", (validPatterns * 100 / len(dictFixed)))
        
