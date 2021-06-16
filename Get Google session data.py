#!/Users/simoncook/opt/anaconda3/envs/Spectator/bin/python
# -*- coding: utf-8 -*-
"""
Created on Wed May 19 10:26:04 2021

@author: simoncook
"""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
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

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'spectator-analytics-0bd98707e1ed.json'
VIEW_ID = '211324222'

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

def get_report(analytics, start_date, end_date, metric_list, dimension_list, pagetoken):
    
# =============================================================================
#     Google API call for data
# =============================================================================
    
    return analytics.reports().batchGet(
      body={
        'reportRequests': {
          'viewId': VIEW_ID,
          'dateRanges': {
              'startDate': start_date.strftime(format = "%Y-%m-%d"), 
              'endDate': end_date.strftime(format = "%Y-%m-%d")
              },
          'metrics': metric_list,
          'dimensions': dimension_list,
          "pageSize" : 1000,
          "pageToken" : pagetoken,
          'samplingLevel':  'LARGE'
        }
      }
  ).execute()


def handle_report(analytics, start_date, end_date, metric_list, dimension_list, pagetoken, rows):  

# =============================================================================
#   Recursive function that deals with pagination    
# =============================================================================

    response = get_report(analytics, start_date, end_date, metric_list, dimension_list, pagetoken)

    columnHeader = response.get("reports")[0].get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    pagetoken = response.get("reports")[0].get('nextPageToken', None)
    rowsNew = response.get("reports")[0].get('data', {}).get('rows', [])
    rows = rows + rowsNew
    print("len(rows): " + str(len(rows)))

    try:
        sample_size = float(response['reports'][0]['data']['samplesReadCounts'][0])
        sample_space = float(response['reports'][0]['data']['samplingSpaceSizes'][0])
        print("sample %:" + str(sample_size/sample_space*100))
    except:
        print("sample data not available")

    if pagetoken != None:
        return handle_report(analytics, start_date, end_date, metric_list, dimension_list, pagetoken,rows)
    else:
        return rows
    
    
    
def get_data():    
    analytics = initialize_analyticsreporting()

    dfanalytics = []

    '''
    #This generated lists of start and end dates to send to API call
    yesterday = dt.datetime.today().date()- dt.timedelta(days=1)
    start_date = dt.date(2021,1,1)
    #yesterday = dt.date(2021,1,31) #just for testing
  
    start_dates = pd.date_range(start = start_date, end = yesterday, freq = '15D')
    end_dates = (start_dates - dt.timedelta(days = 1)).shift(1).date.tolist()
    end_dates[-1] = yesterday
    start_dates = start_dates.date.tolist()
    '''
    
    #Generate list of dates from start_date to yesterday at one day increments
    start_date = dt.date(2020,3,1)
    yesterday = dt.datetime.today().date()- dt.timedelta(days=1)
    end_date = dt.date(2021,5,27)
    start_dates = pd.date_range(start = start_date, end = end_date, freq = '1D').date.tolist()

    #list of dimensions - don't need ga:date as we're just asking for one day's data
    dimensions = ['ga:dimension8']
    
                  #'ga:dimension5',
                  #
                  #'ga:dimension12',
                  #'ga:dimension15',
                  #]
    metrics = ['ga:sessions']
    
    #renaming dictionary for columns
    column_rename = {'ga:deviceCategory':'device category',
                     'ga:socialNetwork': 'social network',
                     'ga:medium':'source medium',
                     'ga:country':'country',
                     'ga:dimension2':'author', 
                     'ga:dimension3':'category',
                     'ga:dimension5':'subsection',
                     'ga:dimension8':'user type',
                     'ga:dimension12':'topics',
                     'ga:dimension15':'amp',
                     'ga:dimension17':'article ID',
                     'ga:uniquePageviews':'unique page views',
                     'ga:sessions':'sessions',
                     'ga:channelGrouping':'channel grouping'}

    col_names = ['device category', 'social network', 'social network',
                 'author', 'category', 'subsection', 'user type', 
                 'topics', 'amp', 'unique page views']

    #dynamically create list of dicts to pass for batchGet
    dimension_list = [{k: v} for k,v in zip(["name"]*len(dimensions), dimensions)]
    
    #do the same for metrics
    metric_list = [{k: v} for k,v in zip(["expression"]*len(metrics), metrics)]

    df_list = []
  
    for i in range(len(start_dates)):
        print(start_dates[i])

        rows = []
        
        feed_updated = False
        while feed_updated == False:
            try: 
                rows = handle_report(analytics, start_dates[i], start_dates[i], metric_list, dimension_list,'0',rows)
                feed_updated=True
            except HTTPError: 
                print("HTTP Error - trying again")
                time.sleep(random()*2) #Just needs more time.
            except:
                print("Unidentified Error - trying again") 
                time.sleep(random()*2) #Just needs more time.

                

        dfanalytics = pd.DataFrame(list(rows))

        dfanalytics[dimensions] = pd.DataFrame(dfanalytics['dimensions'].tolist(), index= dfanalytics.index)
        dfanalytics[metrics[0]] = dfanalytics['metrics'].apply(lambda x: x[0]['values'][0])
        dfanalytics = dfanalytics.drop(["dimensions", "metrics"], axis = 1)

        dfanalytics['date'] = start_dates[i]     
        dfanalytics = dfanalytics.set_index('date')

        dfanalytics = dfanalytics.rename(columns = column_rename)

        #now insert this data into the database
        insert_data(dfanalytics, "Spectator Analytics.db")
        print("data for " + str(start_dates[i]) + " added")
        
        time.sleep(random()*2)

    return 

def add_data(conn, table, df):

    df.to_sql(table, conn, if_exists='append')
       
    return

def insert_data(df, database):
    
    engine = create_engine('sqlite:///' + database, echo=False)
    sqlite_connection = engine.connect()
    
    sqlite_table = "Google Analytics Session Data - users"
    
    add_data(sqlite_connection, sqlite_table, df)  
    
    sqlite_connection.close()
    
    return



if __name__ == '__main__':
    
    #get data from Google API
    df = get_data()

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    