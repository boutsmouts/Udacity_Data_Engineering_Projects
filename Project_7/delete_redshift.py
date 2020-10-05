'''
This python script uses the specified config-file to

    log into AWS using the specified keys
    delete the specified Redshift cluster
    delete the used VPC group
    delete the used IAM user role

Note that the script checks the status of the Redshift cluster and only proceeds when the cluster is finally deleted.

Note that HOST and ROLE_ARN in the config-file are reset to their default values after the deletion is completed.
'''

import boto3
from botocore.exceptions import ClientError
import json
import pandas as pd
import configparser
import psycopg2
import time
from setup_redshift import create_clients

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))



def delete_Redshift(Redshift_instance, config_settings):

    '''
    FUNCTION:   delete_Redshift
    PURPOSE:    Permanently(!) deletes a given Redshift cluster without creating a snapshot.
    INPUT:      Redshift_instance:  a predefined Redshift client
                config_settings:    a configparser to a given config-file
    OUTPUT:     No explicit return, prints to console
    '''

    print('')
    print('Deleting Redshift cluster {}...'.format(config_settings.get('CLUSTER', 'IDENTIFIER')))
    print('-----')
    print('')

    try:
        cluster_status = Redshift_instance.describe_clusters(ClusterIdentifier = config_settings.get('CLUSTER', 'IDENTIFIER'))['Clusters'][0]['ClusterStatus']

        while cluster_status != 'available':
            print('Redshift cluster {} is not ready yet. Please wait...'.format(config_settings.get('CLUSTER', 'IDENTIFIER')))
            time.sleep(20)
            cluster_status = Redshift_instance['redshift'].describe_clusters(ClusterIdentifier = config_settings.get('CLUSTER', 'IDENTIFIER'))['Clusters'][0]['ClusterStatus']

        response = Redshift_instance.delete_cluster(ClusterIdentifier = config_settings.get('CLUSTER', 'IDENTIFIER'),  SkipFinalClusterSnapshot=True)

        try:
            config_settings['CLUSTER']['HOST'] = 'UPDATED_AUTOMATICALLY'
            with open('dwh.cfg', 'w') as cfg_file:
                config_settings.write(cfg_file)

        except Exception as e:
            print('Error writing config file: {}'.format(e))

        print('Cluster deletion successfully initiated! Please wait...')

        cluster_status = 'not deleted'

        while cluster_status != 'deleted':
            try:
                cluster_status = Redshift_instance.describe_clusters(ClusterIdentifier = config_settings.get('CLUSTER', 'IDENTIFIER'))['Clusters'][0]['ClusterStatus']
                time.sleep(10)

            except Exception as e:
                cluster_status = 'deleted'

        print('Cluster {} successfully deleted!'.format(config_settings.get('CLUSTER', 'IDENTIFIER')))

    except Exception as e:
        print('Error deleting cluster: {}'.format(e))



def delete_VPC_group(EC2_instance, config_settings):

    '''
    FUNCTION:   delete_VPC_group
    PURPOSE:    Permanently(!) deletes a given VPC security group
    INPUT:      EC2_instance:       a predefined EC2 client
                config_settings:    a configparser to a given config-file
    OUTPUT:     No explicit return, prints to console
    '''

    print('')
    print('Deleting VPC group {}...'.format(config_settings.get('CLUSTER', 'SECURITY_GROUP_NAME')))
    print('-----')
    print('')

    try:
        response = EC2_instance.delete_security_group(
            GroupId     = EC2_instance.describe_security_groups(Filters = [{'Name': 'group-name', 'Values': [config.get('CLUSTER', 'SECURITY_GROUP_NAME')]}])['SecurityGroups'][0]['GroupId'],
            GroupName   = config_settings.get('CLUSTER', 'SECURITY_GROUP_NAME')
        )

        print('VPC security group {} successfully deleted!'.format(config_settings.get('CLUSTER', 'SECURITY_GROUP_NAME')))

    except Exception as e:
        print('Error deleting VPC security group: {}'.format(e))



def delete_IAM(IAM_instance, config_settings):

    '''
    FUNCTION:   delete_IAM
    PURPOSE:    Permanently(!) deletes a given IAM user role
    INPUT:      IAM_instance:       a specified IAM client
                config_settings:    a configparser to a given config-file
    OUTPUT:     No explicit return, prints to console
    '''

    print('')
    print('Detaching policy {} from IAM role {}...'.format(config_settings.get('IAM', 'POLICY_ARN'), config_settings.get('IAM', 'NAME')))
    print('-----')
    print('')

    try:
        IAM_instance.detach_role_policy(
            RoleName    = config_settings.get('IAM', 'NAME'),
            PolicyArn   = config_settings.get('IAM', 'POLICY_ARN')
        )

        print('Policy {} successfully detached from IAM role {}!'.format(config_settings.get('IAM', 'POLICY_ARN'), config_settings.get('IAM', 'NAME')))

    except Exception as e:
        print('Error while detaching policy from IAM role: {}'.format(e))

    print('')
    print('Deleting IAM role {}...'.format(config_settings.get('IAM', 'NAME')))
    print('-----')
    print('')

    try:
        IAM_instance.delete_role(
            RoleName    = config_settings.get('IAM', 'NAME')
        )

        print('IAM role {} successfully deleted!'.format(config_settings.get('IAM', 'NAME')))

        try:
            config_settings['IAM']['ROLE_ARN'] = 'UPDATED_AUTOMATICALLY'
            with open('dwh.cfg', 'w') as cfg_file:
                config_settings.write(cfg_file)

        except Exception as e:
            print('Error writing config file: {}'.format(e))

    except Exception as e:
        print('Error while deleting IAM role: {}'.format(e))



def main():

    '''
    FUNCTION:   main
    PURPOSE:    Runs the specified functions to delete a Redshift cluster
    INPUT:      None
    OUTPUT:     None
    '''

    clients     = create_clients(config)
    response    = delete_Redshift(clients['redshift'], config)
    response    = delete_VPC_group(clients['ec2'], config)
    response    = delete_IAM(clients['iam'], config)

if __name__ == '__main__':
    main()
