# -*- coding: utf-8 -*-
"""
Created on Mon Sep 18 20:10:27 2017

@author: DanLo1108
"""

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import urllib2
import re
import sklearn
import string as st
import matplotlib.pyplot as plt
import os
import string as st

#url='http://www.espn.com/nba/playbyplay?gameId=400899617'

import sqlalchemy as sa
import pymysql



#General Board Topic URLs
url='https://forums.realgm.com/boards/viewforum.php?f=6&start=0#start_here'
    
request=urllib2.Request(url)
page = urllib2.urlopen(request)


content=page.read()
soup=BeautifulSoup(content,'lxml')


urls=soup.find_all('a',href=True)

topic_urls=pd.unique(['https://forums.realgm.com/boards/'+url['href'] for url in urls if 'viewtopic' in url['href'] and '&p' not in url['href'] and 'start' not in url['href']])



