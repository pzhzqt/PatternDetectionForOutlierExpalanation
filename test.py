#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 02:21:26 2017

@author: sushmitasinha
"""

from PatternFinder import PatternFinder

values = ["pubcount"]
#values = ['open', 'high', 'low', 'volume']
time = ["year"]
dimensions = []
categories = ["name","pubkey"]
tableName = "pub_select"
p = PatternFinder(time, categories, dimensions, values, tableName)

p.findPatterns()
