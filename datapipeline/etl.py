import configparser
import psycopg2
import boto3
import pandas as pd
import numpy
import fastparquet
#import pyarrow
from sql_queries import  drop_table_queries, create_table_queries, insert_querires,drop_raw_queries

def drop_tables(cur, conn):
    '''
    this function drops tables if exists
    '''
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    '''
    this function create dummy tables
    '''
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def data_cleaning(s3):
    '''
    this function load and clean raw data, save it as txt, and upload to s3
    '''
    
    #i94
    raw_i94 = pd.read_csv('../rawdata/immigration_data_sample.csv')
    raw_i94 = raw_i94.drop(raw_i94.columns[0], axis=1)
    i94=raw_i94[['cicid'
                    ,'i94yr'
                    ,'i94mon'
                    ,'i94cit'
                    ,'i94res'
                    ,'i94port'
                    ,'arrdate'
                    ,'i94mode'
                    ,'i94addr'
                    ,'depdate'
                    ,'i94visa'
                    ,'dtadfile'
                    ,'visapost'
                    ,'dtaddto'
                    ,'airline'
                    ,'admnum'
                    ,'fltno'
                    ,'visatype']]
    i94['arrdate'] = pd.to_timedelta(i94['arrdate'],unit='D') + pd.Timestamp('1960-1-1')
    i94['depdate'] = pd.to_timedelta(i94['depdate'],unit='D') + pd.Timestamp('1960-1-1')
    i94.to_csv('../dataoutput/i94.txt',index = False)
    s3.Bucket('immigrationdatamodel').upload_file(Filename='../dataoutput/i94.txt', Key='i94.txt')
    
    #citydemo
    raw_citydemo = pd.read_csv('../rawdata/us-cities-demographics.csv',sep = ';')
    citydemo_unchanged = raw_citydemo.iloc[:,0:10].drop_duplicates()
    citydemo_stacked = raw_citydemo.iloc[:,[0,1,10,11]]
    citydemo_stacked['Race'] = citydemo_stacked['Race'].replace(['American Indian and Alaska Native',
                                               'Black or African-American',
                                               'Hispanic or Latino'],
                                              ['Native','Black','Hispa'])
    citydemo_unstacked = citydemo_stacked.pivot_table(index=['City','State'], 
                                                          columns="Race", 
                                                          values="Count", 
                                                          aggfunc='first').reset_index()
    citydemo = citydemo_unchanged.merge(citydemo_unstacked,on = ['City','State'])
    citydemo.columns = citydemo.columns.str.replace(' ', '')
    citydemo.columns = citydemo.columns.str.replace('-', '')
    citydemo['citystate']=citydemo['City'].str.lower().str.replace(" ","")+citydemo['StateCode'].str.lower().str.replace(" ","")
    
    citydemo.to_csv('../dataoutput/citydemo.txt',index = False)
    s3.Bucket('immigrationdatamodel').upload_file(Filename='../dataoutput/citydemo.txt', Key='citydemo.txt')
    
    #airport
    raw_airportcode = pd.read_csv('../rawdata/airport-codes_csv.csv')
    airportcode = raw_airportcode.assign(iso_region2 = lambda x: x['iso_region'].str.split('-',1).str[-1])
    airportcode[['lat','lon']] = airportcode['coordinates'].str.split(',',1,expand = True)
    airportcode['citystate']=airportcode['municipality'].str.lower().str.replace("","")+airportcode['iso_region2'].str.lower().str.replace(" ","")
    
    airportcode.to_csv('../dataoutput/airport.txt',index = False)
    s3.Bucket('immigrationdatamodel').upload_file(Filename='../dataoutput/airport.txt', Key='airport.txt')
    
    #usport
    raw_port = pd.read_csv('../rawdata/USport.txt',sep = '|')
    raw_port['citystate']=raw_port['portname'].str.lower().str.replace(" ","")+raw_port['state'].str.lower().str.replace(" ","")
    raw_port.to_csv('../dataoutput/usport.txt',index = False)
    s3.Bucket('immigrationdatamodel').upload_file(Filename='../dataoutput/usport.txt', Key='usport.txt')
    
    #countries
    raw_countries = pd.read_csv('../rawdata/countries.txt',sep = '|')
    raw_countries.to_csv('../dataoutput/countries.txt',index = False)
    s3.Bucket('immigrationdatamodel').upload_file(Filename='../dataoutput/countries.txt', Key='countries.txt')

def insert_tables(cur, conn):
    '''
    this function create dummy tables
    '''

    for query in insert_querires:
        cur.execute(query)
        conn.commit()

def cleanup(cur,conn):
    for query in drop_raw_queries:
        cur.execute(query)
        conn.commit()

def main():
    '''
    this is the main function of this script
    it grabs configurations from dwh.cfg file, set up connection to redshift cluster
    then run load_staging_tables and insert_tables on set-up connection
    '''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    conn_string="postgresql://{}:{}@{}:{}/{}".format(config.get("CLUSTER","DB_USER"),
                                                   config.get("CLUSTER",'DB_PASSWORD'),
                                                   config.get("CLUSTER",'HOST'),
                                                   config.get("CLUSTER",'DB_PORT'),
                                                   config.get("CLUSTER",'DB_NAME'))

    s3path = config.get('S3','LOCATION')
    
    s3 = boto3.resource(
    service_name='s3',
    region_name='us-west-2',
    aws_access_key_id=config.get("AWS","KEY"),
    aws_secret_access_key=config.get("AWS","SECRET")
    )
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    data_cleaning(s3)
    insert_tables(cur, conn)
    cleanup(cur,conn)
    
    conn.close()



if __name__ == "__main__":
    main()
