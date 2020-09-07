import pandas as pd
import boto3
import json
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

### IMPORT CONFIGS ###

KEY                     = config.get('AWS', 'KEY')
SECRET                  = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE        =
DWH_NUM_NODES           =
DWH_NODE_TYPE           =

DWH_CLUSTER_IDENTIFIER  =
DWH_DB                  =
DWH_DB_USER             =
DWH_DB_PASSWORD         =
DWH_PORT                =

DWH_IAM_ROLE_NAME       =

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({'Param':
                    ['DWH_CLUSTER_TYPE', 'DWH_NUM_NODES', 'DWH_NODE_TYPE', 'DWH_CLUSTER_IDENTIFIER', 'DWH_DB', 'DWH_DB_USER', 'DWH_DB_PASSWORD', 'DWH_PORT', 'DWH_IAM_ROLE_NAME'],
              'Value':
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
            })
