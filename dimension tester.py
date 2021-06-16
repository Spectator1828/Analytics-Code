#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 17 14:54:57 2021

@author: simoncook
"""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import datetime as dt
import time
from random import random
from github import Github

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'spectator-analytics-0bd98707e1ed.json'
VIEW_ID = '211324222' #spectator.co.uk
#VIEW_ID = '228416300' #data.spectator.co.uk


g = Github("1e941b45145bf86c165f90935165fa6e8fd48123")
repo = g.get_user().get_repo("SpecProj")

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


def get_report_test(analytics, start_date, end_date):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': start_date.strftime(format = "%Y-%m-%d"), 
                          'endDate': end_date.strftime(format = "%Y-%m-%d")}],
          'metrics': [{'expression': 'ga:sessions'}],
          'dimensions': [{'name': 'ga:date'},
                         {'name': 'ga:socialNetwork'}],
                          #'name': 'ga:eventlabel'}], #, {'name': 'ga:dimension8'},],
          'dimensionFilterClauses':[{
              #'operator':'AND',
              'filters': [{'dimensionName' : 'ga:socialNetwork',
                           'operator':'EXACT',
                           'expressions':'Twitter'  
                        }
                          
                          ]
                       }],
          'samplingLevel':  'LARGE'
        }]
      }
  ).execute()


'''
 

'dimensionFilterClauses':[{
              'operator':'AND',
              'filters': [{'dimensionName' : 'ga:eventAction',
                           'operator':'EXACT',
                           'expressions':'User Subscribed'  
                        }
                          
                          ]
                       }],



{'dimensionName':'ga:eventCategory',
                       'operator':'EXACT',
                       'expressions':'Subscription'              
                       }

'''



def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
  dim_list = []
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        print(header + ': ', dimension)
        dim_list = dim_list + [dimension]

      for i, values in enumerate(dateRangeValues):
        #print('Date range:', str(i))
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          print(metricHeader.get('name') + ':', value)

    #just lose the dates
    dim_list = dim_list[1::2]

    return dim_list

def main():
  start_date = dt.date(2021,2,1)
  end_date = dt.date(2021,5,24)
  #end_date= start_date 
  
  analytics = initialize_analyticsreporting()
  response = get_report_test(analytics, start_date, end_date)
  dim_list = print_response(response)
  
  
  return dim_list




if __name__ == '__main__':
    
    dim_list = main()