import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_PATH     = config.get('S3','LOCATION')
IAM_ROLE     = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

raw_i94_drop = "DROP TABLE IF EXISTS raw_i94"
i94_drop = "DROP TABLE IF EXISTS i94"
countries_drop = "DROP TABLE IF EXISTS countries"
USport_drop = "DROP TABLE IF EXISTS USport"
cic_drop = "DROP TABLE IF EXISTS cic"
citydemo_drop = "DROP TABLE IF EXISTS citydemo"
airport_drop = "DROP TABLE IF EXISTS airport"

# CREATE cic TABLES

cic_create= ("""
CREATE TABLE IF NOT EXISTS cic
(cicid float, 
biryear float, 
occup varchar, 
visatype varchar)
""")

raw_i94_create=("""
CREATE TABLE IF NOT EXISTS raw_i94
(cicid float
,i94yr varchar
,i94mon varchar
,i94cit varchar
,i94res varchar
,i94port varchar
,arrdate varchar
,i94mode varchar
,i94addr varchar
,depdate varchar
,i94bir varchar
,i94visa varchar
,count varchar
,dtadfile varchar
,visapost varchar
,occup varchar
,entdepa varchar
,entdepd varchar
,entdepu varchar
,matflag varchar
,biryear float
,dtaddto varchar
,gender varchar
,insnum varchar
,airline varchar
,admnum varchar
,fltno varchar
,visatype varchar
)
""")

i94_create=("""
CREATE TABLE IF NOT EXISTS i94
(cicid float
,i94yr Float
,i94mon Float
,i94cit Float
,i94res Float
,i94port Varchar
,arrdate Varchar
,i94mode Varchar
,i94addr Varchar
,depdate Varchar
,i94visa Varchar
,dtadfile Varchar
,visapost Varchar
,dtaddto Varchar
,airline Varchar
,admnum Varchar
,fltno Varchar
,visatype Varchar
)
""")

citydemo_create=("""
CREATE TABLE IF NOT EXISTS citydemo
(
City Varchar
,State Varchar
,MedianAge Float
,MalePopulation Float
,FemalePopulation Float
,TotalPopulation Float
,NumberofVeterans Float
,Foreignborn Float
,AverageHouseholdSize Float
,StateCode Varchar
,Asian Float
,Black Float
,Hispa Float
,Native Float
,White Float
,citystate varchar 
,PRIMARY KEY (citystate)
)
""")


airport_create=("""
CREATE TABLE IF NOT EXISTS airport
(
ident Varchar
,type Varchar
,name Varchar
,elevation_ft Float
,continent Varchar
,iso_country Varchar
,iso_region Varchar
,municipality Varchar
,gps_code Varchar
,iata_code Varchar
,local_code Varchar
,coordinates Varchar
,iso_region2 Varchar
,lat Float
,lon Float
,citystate varchar 
,PRIMARY KEY (citystate)
)
""")



countries_create=("""
CREATE TABLE IF NOT EXISTS countries
(
countrycode float
,country Varchar
,PRIMARY KEY (countrycode)
)
""")


usport_create=("""
CREATE TABLE IF NOT EXISTS usport
(
port varchar
,portname Varchar
,state varchar
,citystate varchar 
,PRIMARY KEY (port)
)
""")


# insert cic table from i94 raw table

cic_insert = ("""  
    INSERT INTO cic (cicid, 
                           biryear, 
                           occup, 
                           visatype)
    SELECT DISTINCT cicid, 
                           biryear, 
                           occup, 
                           visatype
    FROM raw_i94
""")


raw_i94_copy = ("""
    copy raw_i94 from '{}/raw_i94.txt'
    credentials 'aws_iam_role={}'
    csv
    IGNOREHEADER 1
""").format(S3_PATH, IAM_ROLE)

i94_copy = ("""
    copy i94 from '{}/i94.txt'
    credentials 'aws_iam_role={}'
    csv
    IGNOREHEADER 1
""").format(S3_PATH, IAM_ROLE)

citydemo_copy = ("""
    copy citydemo from '{}/citydemo.txt'
    credentials 'aws_iam_role={}'
    csv
    IGNOREHEADER 1
""").format(S3_PATH, IAM_ROLE)

airport_copy = ("""
    copy airport from '{}/airport.txt'
    credentials 'aws_iam_role={}'
    csv
    IGNOREHEADER 1
""").format(S3_PATH, IAM_ROLE)

countries_copy = ("""
    copy countries from '{}/countries.txt'
    credentials 'aws_iam_role={}'
    csv
    IGNOREHEADER 1
""").format(S3_PATH, IAM_ROLE)

usport_copy = ("""
    copy usport from '{}/usport.txt'
    credentials 'aws_iam_role={}'
    csv
    IGNOREHEADER 1
""").format(S3_PATH, IAM_ROLE)


#drop i94 raw table

raw_i94_drop = "DROP TABLE IF EXISTS raw_i94"


# QUERY LISTS

drop_table_queries = [raw_i94_drop,i94_drop, countries_drop, USport_drop,cic_drop ,citydemo_drop,airport_drop]
create_table_queries = [cic_create,raw_i94_create,i94_create, citydemo_create,airport_create,countries_create,usport_create]
insert_querires = [raw_i94_copy,i94_copy,citydemo_copy,airport_copy,countries_copy,usport_copy,cic_insert]
drop_raw_queries = [raw_i94_drop]