#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 17:31:59 2022

@author: kent
"""

import pandas as pd
import matplotlib.pyplot as plt

df = pd.DataFrame([['g1','c1',10],['g1','c2',12],['g1','c3',13],['g2','c1',8],
                   ['g2','c2',10],['g2','c3',12]],columns=['group','column','val'])

df1 = pd.DataFrame([['g1','c1',0],['g1','c2',0],['g1','c3',0],['g2','c1',8],
                   ['g2','c2',0],['g2','c3',0]],columns=['group','column','val'])

alldf = pd.merge(df,df1, on = ['group','column'], how = 'left')
wikimeltdf = pd.melt(alldf, id_vars=['group','column'], value_vars=['val_x','val_y'],
                   var_name='source', value_name='percentage')

df.pivot("column", "group", "val").plot(kind='bar')

plt.show()

df.pivot("column", "group", "val")