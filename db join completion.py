#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 01:27:58 2021

@author: simoncook
"""

import pandas as pd
import datetime as dt
import time
from random import random
from github import Github
import json
from requests.exceptions import HTTPError

import sqlite3
from sqlite3 import Error
from sqlalchemy import create_engine

def read_data(query, conn):
    
    df = pd.read_sql(query, con=conn)
    
    return df


def main():

    engine = create_engine('sqlite:///Spectator Analytics.db', echo=False)
    sqlite_connection = engine.connect()
    
    start_date = dt.date(2021,1,1)
    end_date =  dt.date(2021,6,17)
    
    start_date_str = start_date.strftime(format = "%Y-%m-%d")
    end_date_str = end_date.strftime(format = "%Y-%m-%d")
    
    sql_query = """
                SELECT * 
                FROM "join page country device"
                WHERE (date BETWEEN '{start_date}' AND '{end_date}')

                """.format(start_date = start_date_str, end_date = end_date_str)
    
    df_joinpage_views = read_data(sql_query, sqlite_connection).set_index('date')
    
    df_UK = df_joinpage_views[df_joinpage_views['country']=='United Kingdom']
    df_UK = df_UK.groupby(df_UK.index).sum().rename(columns={'unique page views':'UK'})
    df_notUK = df_joinpage_views[df_joinpage_views['country']!='United Kingdom']
    df_notUK = df_notUK.groupby(df_notUK.index).sum().rename(columns={'unique page views':'non UK'})
    
    df_joinpage_views_UK = pd.concat([df_UK, df_notUK], axis = 1)
    
    df_joinpage_views_by_device = df_joinpage_views.groupby([df_joinpage_views.index, 'device category']).sum().reset_index()
    df_joinpage_views_by_device = df_joinpage_views_by_device.pivot_table(index = 'date', columns = 'device category', values = 'unique page views')
    
    sql_query = """
                SELECT * 
                FROM "subscriber country device"
                WHERE (date BETWEEN '{start_date}' AND '{end_date}')

                """.format(start_date = start_date_str, end_date = end_date_str)
    
    df_subscriptions = read_data(sql_query, sqlite_connection).set_index('date')
    
    df_UK = df_subscriptions[df_subscriptions['country']=='United Kingdom']
    df_UK = df_UK.groupby(df_UK.index).sum().rename(columns={'ga:uniqueEvents':'UK'})
    df_notUK = df_subscriptions[df_subscriptions['country']!='United Kingdom']
    df_notUK = df_notUK.groupby(df_notUK.index).sum().rename(columns={'ga:uniqueEvents':'non UK'})
    
    df_subscriptions_UK = pd.concat([df_UK, df_notUK], axis = 1)
    
    df_subscriptions_by_device = df_subscriptions.groupby([df_subscriptions.index, 'device category']).sum().reset_index()
    df_subscriptions_by_device = df_subscriptions_by_device.pivot_table(index = 'date', columns = 'device category', values = 'ga:uniqueEvents')
    
    df_UK = df_subscriptions_UK / df_joinpage_views_UK *100
    df_UK.to_csv('completed subscriptions UK.csv')

    df_by_device = df_subscriptions_by_device / df_joinpage_views_by_device *100
    df_by_device.to_csv('completed subscriptions by device.csv')

    sqlite_connection.close()

    return 



if __name__ == '__main__':
    
    main()