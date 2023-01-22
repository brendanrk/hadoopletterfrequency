#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# Libraries
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import subprocess
from io import StringIO

def retrieve_df(hf):
    cmd = "docker exec myhadoop hdfs dfs -cat myOutput/{}".format(hf)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    DATA = StringIO(out.decode())
    df = pd.read_csv(DATA, sep="\t", names=["language", "letter", "frequency"])
    df  = (df
             .query('frequency > 0.0000')
          )
    return df




def show_chart(df):
    fig = plt.figure(figsize=(15,10))

    ax = fig.add_subplot(111)
    ax.set_title(df["language"].iloc[0])
    # Make a random dataset:
    height = df["frequency"]
    bars = df["letter"]
    y_pos = np.arange(len(bars))
    # Create bars
    ax.bar(y_pos, height)
    # Create names on the x-axis
    plt.xticks(y_pos, bars)
    plt.xlabel("letter")
    plt.ylabel("fraction")
    # Show graphic
    plt.show()


languages = ["fr", "de", "es", "it"]
hadoop_files = ["part-r-00000","part-r-00001","part-r-00002","part-r-00003"]
for lang, hf in dict(zip(languages, hadoop_files)).items():
    df = retrieve_df(hf)
    show_chart(df)