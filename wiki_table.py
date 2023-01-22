#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import requests
from bs4 import BeautifulSoup
import pandas as pd

def get_wiki_data():
    URL = "https://en.wikipedia.org/wiki/Letter_frequency"
    
    content = requests.get(URL).text
    # print(res)
    soup = BeautifulSoup(content, 'html.parser')
    # tables = soup.findAll("table", class_="wikitable sortable jquery-tablesorter")
    tables = soup.findAll("table",{"class":"wikitable sortable"})
    
    for i, table in enumerate(tables):
        headings = [th.get_text().strip() for th in table.find("tr").find_all("th")]
        # print(headings)
        
        if table.findParent("table") is None:
            # print(i, str(table))
            # print(type(table))
            datasets = []
            for row in table.find_all("tr")[1:]:
                dataset = dict(zip(headings, (td.get_text().strip().replace("%","") for td in row.find_all("td"))))
                datasets.append(dataset)
            # print(datasets)
    
    
    df = pd.DataFrame(datasets)
    df = df.filter(['Letter','French[21]', 'Spanish[23]', 'Italian[26]', 'German[22]'])
    print(df['Letter'].tolist())
    return df