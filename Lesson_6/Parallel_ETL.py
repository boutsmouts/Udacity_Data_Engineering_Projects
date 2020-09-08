import pandas as pd
import boto3
import json
import configparser
from time import time
import matplotlib.pyplot as plt
%load_ext sql

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                     = config.get('AWS', 'KEY')
SECRET                  = config.get('AWS', 'SECRET')

DWH_CLUSTER_IDENTIFIER  = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB                  = config.get('DWH', 'DWH_DB')
DWH_DB_USER             = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD         = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT                = config.get('DWH', 'DWH_PORT')

DWH_IAM_ROLE_NAME       = config.get('DWH', 'DWH_IAM_ROLE_NAME')

redshift = boto3.client('redshift',
                       region_name="eu-central-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='eu-central-1'
                  )

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

if redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus'] == 'available':
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", roleArn)



conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string

sampleDbBucket = s3.Bucket('udacity-labs')

for obj in sampleDbBucket.objects.filter(Prefix = 'tickets'):
    print(obj)

### CREATE TABLE FOR PARTITIONED DATA ###

%%sql
DROP TABLE IF EXISTS sporting_event_ticket;
CREATE TABLE sporting_event_ticket (
    id double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    sporting_event_id double precision NOT NULL,
    sport_location_id double precision NOT NULL,
    seat_level numeric(1,0) NOT NULL,
    seat_section character varying(15) NOT NULL,
    seat_row character varying(10) NOT NULL,
    seat character varying(10) NOT NULL,
    ticketholder_id double precision,
    ticket_price numeric(8,2) NOT NULL
);

### LOAD PARTITIONED DATA INTO THE CLUSTER ###

%%time
query = '''
        copy sporting_event_ticket from 's3://udacity-labs/tickets/split/part'
        credentials 'aws_iam_role={}'
        gzip delimiter ';' compupdate off region 'us-west-2';
        '''.format(DWH_ROLE_ARN)

%sql $query

### CREATE TABLE FOR NON-PARTITIONED DATA ###

%%sql
DROP TABLE IF EXISTS sporting_event_ticket_full;
CREATE TABLE sporting_event_ticket_full (
    id double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    sporting_event_id double precision NOT NULL,
    sport_location_id double precision NOT NULL,
    seat_level numeric(1,0) NOT NULL,
    seat_section character varying(15) NOT NULL,
    seat_row character varying(10) NOT NULL,
    seat character varying(10) NOT NULL,
    ticketholder_id double precision,
    ticket_price numeric(8,2) NOT NULL
);

### LOAD NON-PARTITIONED DATA INTO THE CLUSTER (MUCH SLOWER!) ###

%%time
query = '''
        copy sporting_event_ticket_full from 's3://udacity-labs/tickets/full/full.csv.gz'
        credentials 'aws_iam_role={}'
        gzip delimiter ';' compupdate off region 'us-west-2';
        '''.format(DWH_ROLE_ARN)

%sql $query

### PAUSE AND RESUME CLUSTER ###

#redshift.pause_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)

#redshift.resume_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
