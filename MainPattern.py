#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from PatternFinder import PatternFinder

values = ["pubcount"]
#values = ['open', 'high', 'low', 'volume']
time = ["year"]
dimensions = []
categories = ["name","pubkey"]
tableName = "pub_select"
p = PatternFinder(time, categories, dimensions, values, tableName)

p.findPatterns()