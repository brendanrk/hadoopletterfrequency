#!/usr/bin/env python3
# Libraries
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import subprocess
from io import StringIO
import requests
from bs4 import BeautifulSoup

def get_wiki_data():
    URL = "https://en.wikipedia.org/wiki/Letter_frequency"
    
    content = requests.get(URL).text
    # print(res)
    soup = BeautifulSoup(content, 'html.parser')
    # tables = soup.findAll("table", class_="wikitable sortable jquery-tablesorter")
    tables = soup.findAll("table",{"class":"wikitable sortable"})
    
    for i, table in enumerate(tables):
        headings = [th.get_text().strip() for th in table.find("tr").find_all("th")]     
        if table.findParent("table") is None:
            datasets = []
            for row in table.find_all("tr")[1:]:
                dataset = dict(zip(headings, (td.get_text().strip().replace("%","").replace("~","").replace(")","").replace("(","") for td in row.find_all("td"))))
                datasets.append(dataset)
    
    df = pd.DataFrame(datasets)
    df = df.astype({'French[21]':float, 'Spanish[23]':float, 'Italian[26]':float, 'German[22]':float})
    df = df.filter(['Letter','French[21]', 'Spanish[23]', 'Italian[26]', 'German[22]'])
    print(df['Letter'].tolist())
    df.rename(columns={'Letter':'letter','French[21]': 'fr', 'Spanish[23]': 'es', 'Italian[26]': 'it', 'German[22]': 'de'}, inplace=True)
    return df


def retrieve_df(hf):
    """Get the data stored on HDFS at the arguments location"""
    cmd = "docker exec myhadoop hdfs dfs -cat myOutput/{}".format(hf)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    DATA = StringIO(out.decode())
    df = pd.read_csv(DATA, sep="\t", names=["language", "letter", "percentage"])
    return df




def show_chart(df):
    """Create a bar chart of percentage vs letter"""
    fig = plt.figure(figsize=(15,10))

    ax = fig.add_subplot(111)
    ax.set_title(df["language"].iloc[0], fontsize=20)
    height = df["percentage"]
    bars = df["letter"]
    y_pos = np.arange(len(bars))
    ax.bar(y_pos, height)
    plt.xticks(y_pos, bars)
    plt.xlabel("letter")
    plt.ylabel("percentage")
    plt.show()


wikidf = get_wiki_data()
hadoop_files = ["part-r-00000","part-r-00001","part-r-00002","part-r-00003"]
frames = []
for hf in hadoop_files:
    df = retrieve_df(hf)
    show_chart(df)
    frames.append(df)

resultdf = pd.concat(frames)
wikimeltdf = pd.melt(wikidf, id_vars=['letter'], value_vars=['fr','es','it','de'],
                   var_name='language', value_name='percentage')

allmergedf = wikimeltdf.merge(resultdf, on = ['letter','language'], how = 'left', suffixes=('_wiki', '_gutenburg'))
alldf = pd.melt(allmergedf, id_vars=['letter','language'], value_vars=['percentage_wiki','percentage_gutenburg'],
                   var_name='source', value_name='percentage')

def source_chart(alldf, lang):
    fig = plt.figure(figsize=(15,10))
    ax = fig.add_subplot(111)
    ax.set_title(lang, fontsize=20)
    df = (alldf
          .query('language == "{}"'.format(lang))
          .query('percentage > 0.1')
         )
    df.pivot("letter", "source", "percentage").plot(kind='bar', ax=ax)
    plt.xlabel("letter")
    plt.ylabel("percentage")
    plt.show()

source_chart(alldf, "es")
source_chart(alldf, "de")
source_chart(alldf, "fr")
source_chart(alldf, "de")

allmergedf['difference'] = abs(allmergedf['percentage_wiki'] - allmergedf['percentage_gutenburg'])
print(allmergedf.sort_values('difference', ascending=False).set_index('letter').head(30))
