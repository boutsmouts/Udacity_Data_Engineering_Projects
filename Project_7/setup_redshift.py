'''
This python script uses the specified config-file to

    log into AWS using KEY and SECRET,
    create an IAM role or use an existing one with given name,
    setup a redshift cluster with specifications in the config-file,
    open its specified port for remote connection,
    and check that the cluster and its connectivity are working as intended.

The region of AWS services is predefined as 'us-west-2', but can be changed in the config-file.

Note that for convenience, the ARN of the created IAM role as well as
the cluster endpoint are written automatically into the the specified config-file to ease later use

User should have read and write permission for config-file.
'''

import boto3
from botocore.exceptions import ClientError
import json
import pandas as pd
import configparser
import psycopg2
import time

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))



def create_clients(config_settings):

    '''
    FUNCTION:   create_clients
    PURPOSE:    Creates several defined AWS clients for later use
    INPUT:      config_settings:    a configparser to a given config-file
    OUTPUT:     clients:            a dictionary containing all clients
    '''

    clients_list    = ['ec2', 's3', 'iam', 'redshift']
    clients       = dict()

    for i in clients_list:

        clients.update({i: boto3.client(i,
            region_name            = config_settings.get('CLUSTER', 'REGION'),
            aws_access_key_id      = config_settings.get('AWS', 'KEY'),
            aws_secret_access_key  = config_settings.get('AWS', 'SECRET')
            )
        })

    return clients



def create_IAM(IAM_instance, config_settings):

    '''
    FUNCTION:   create_Redshift
    PURPOSE:    Creates a new IAM user role or uses an existing one
    INPUT:      IAM_instance:       a specified IAM client
                config_settings:    a configparser to a given config-file
    OUTPUT:     IAM_role:           the created IAM user role
    '''

    print('')
    print('Creating new IAM role {}...'.format(config_settings.get('IAM', 'NAME')))
    print('-----')
    print('')

    try:
        IAM_role = IAM_instance.create_role(
            Path                        = '/',
            RoleName                    = config_settings.get('IAM', 'NAME'),
            Description                 = 'Allows Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument    = json.dumps(
                                            {'Statement': [{'Action': 'sts:AssumeRole',
                                             'Effect': 'Allow',
                                             'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                             'Version': '2012-10-17'})
        )

        print('IAM role {} successfully created!'.format(config_settings.get('IAM', 'NAME')))

    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('IAM role already exists! No new role created.')

            IAM_role = IAM_instance.get_role(RoleName = config_settings.get('IAM', 'NAME'))

        else:
            print('Error while creating IAM role: {}'.format(e))

    print('')
    print('Attaching policy {} to IAM role {}...'.format(config_settings.get('IAM', 'POLICY_ARN'), config_settings.get('IAM', 'NAME')))
    print('-----')
    print('')

    try:
        IAM_instance.attach_role_policy(
            RoleName    = config_settings.get('IAM', 'NAME'),
            PolicyArn   = config_settings.get('IAM', 'POLICY_ARN')
        )

        print('Policy {} successfully attached to IAM role {}!'.format(config_settings.get('IAM', 'POLICY_ARN'), config_settings.get('IAM', 'NAME')))

        try:
            config_settings['IAM']['ROLE_ARN'] = IAM_instance.get_role(RoleName = config.get('IAM', 'NAME'))['Role']['Arn']
            with open('dwh.cfg', 'w') as cfg_file:
                config_settings.write(cfg_file)

        except Exception as e:
            print('Error writing config file: {}'.format(e))

    except Exception as e:
        print('Error while attaching policy to IAM role: {}'.format(e))

    return IAM_role


def create_VPC_group(EC2_instance, config_settings):

    '''
    FUNCTION:   create_VPC_group
    PURPOSE:    Creates a new VPC group or uses an existing one
    INPUT:      EC2_instance:       a predefined EC2 client
                config_settings:    a configparser to a given config-file
    OUTPUT:     No explicit return, prints to console
    '''

    print('')
    print('Looking for VPC security group {}...'.format(config.get('CLUSTER', 'SECURITY_GROUP_NAME')))
    print('-----')
    print('')

    try:
        security_group_check = EC2_instance.describe_security_groups(Filters = [{'Name': 'group-name', 'Values': [config.get('CLUSTER', 'SECURITY_GROUP_NAME')]}])['SecurityGroups']

        if not security_group_check:
            print('')
            print('Specified security group does not exist. Creating new one...')
            print('-----')
            print('')

            response = EC2_instance.create_security_group(
                Description = 'Security group to access Redshift via port {}'.format(config_settings.get('CLUSTER', 'DB_PORT')),
                GroupName   = config_settings.get('CLUSTER', 'SECURITY_GROUP_NAME'),
                VpcId       = EC2_instance.describe_security_groups()['SecurityGroups'][0]['VpcId']
            )

            EC2_instance.authorize_security_group_ingress(
                GroupId     = EC2_instance.describe_security_groups(Filters = [{'Name': 'group-name', 'Values': [config.get('CLUSTER', 'SECURITY_GROUP_NAME')]}])['SecurityGroups'][0]['GroupId'],
                GroupName   = config_settings.get('CLUSTER', 'SECURITY_GROUP_NAME'),
                CidrIp      = '0.0.0.0/0',
                IpProtocol  = 'TCP',
                FromPort    = int(config_settings.get('CLUSTER', 'DB_PORT')),
                ToPort      = int(config_settings.get('CLUSTER', 'DB_PORT'))
            )

            print('Security group successfully created and ingress rights correctly set!')

        else:

            print('Specified security group exists, using it for Redshift cluster.')

    except Exception as e:
        print('Error creating security group: {}'.format(e))


def create_Redshift(IAM_role, Redshift_instance, EC2_instance, config_settings):

    '''
    FUNCTION:   create_Redshift
    PURPOSE:    Creates a new Redshift cluster with the given settings, IAM user role, and VPC group
    INPUT:      IAM_role:           a specified IAM user role
                Redshift_instance:  a predefined Redshift client
                EC2_instance:       a predefined EC2 client
                config_settings:    a configparser to a given config-file
    OUTPUT:     No explicit return, prints to console
    '''

    print('')
    print('Creating new Redshift cluster {}...'.format(config_settings.get('CLUSTER', 'IDENTIFIER')))
    print('-----')
    print('')

    try:
        response = Redshift_instance.create_cluster(

            ClusterType         = config_settings.get('CLUSTER', 'TYPE'),
            NodeType            = config_settings.get('CLUSTER', 'NODE_TYPE'),
            NumberOfNodes       = int(config_settings.get('CLUSTER', 'NUM_NODES')),

            DBName              = config_settings.get('CLUSTER', 'DB_NAME'),
            ClusterIdentifier   = config_settings.get('CLUSTER', 'IDENTIFIER'),
            MasterUsername      = config_settings.get('CLUSTER', 'DB_USER'),
            MasterUserPassword  = config_settings.get('CLUSTER', 'DB_PASSWORD'),

            IamRoles            = [IAM_role['Role']['Arn']],

            VpcSecurityGroupIds = [
                                    EC2_instance.describe_security_groups(Filters = [{'Name': 'group-name', 'Values': [config.get('CLUSTER', 'SECURITY_GROUP_NAME')]}])['SecurityGroups'][0]['GroupId']
                                ]
        )

        print('Redshift cluster {} created successfully! Checking status...'.format(config_settings.get('CLUSTER', 'IDENTIFIER')))

        cluster_status = Redshift_instance.describe_clusters(ClusterIdentifier = config_settings.get('CLUSTER', 'IDENTIFIER'))['Clusters'][0]['ClusterStatus']

        while cluster_status != 'available':
            print('Redshift cluster {} is starting. Please wait...'.format(config_settings.get('CLUSTER', 'IDENTIFIER')))
            time.sleep(30)
            cluster_status = Redshift_instance.describe_clusters(ClusterIdentifier = config_settings.get('CLUSTER', 'IDENTIFIER'))['Clusters'][0]['ClusterStatus']

        print('Redshift cluster {} is now available!'.format(config_settings.get('CLUSTER', 'IDENTIFIER')))

        try:
            config_settings['CLUSTER']['HOST'] = Redshift_instance.describe_clusters(ClusterIdentifier = config_settings.get('CLUSTER', 'IDENTIFIER'))['Clusters'][0]['Endpoint']['Address']
            with open('dwh.cfg', 'w') as cfg_file:
                config_settings.write(cfg_file)

        except Exception as e:
            print('Error writing config file: {}'.format(e))

    except Exception as e:
        print('Error creating Redshift cluster: {}'.format(e))


def check_connectivity(Redshift_instance, config_settings):

    '''
    FUNCTION:   check_connectivity
    PURPOSE:    Checks if connection to a given Redshift cluster can be established
    INPUT:      Redshift_instance:  a predefined Redshift client
                config_settings:    a configparser to a given config-file
    OUTPUT:     No explicit return, prints to console
    '''

    print('')
    print('Testing connectivity of Redshift cluster {}...'.format(config.get('CLUSTER', 'IDENTIFIER')))
    print('-----')
    print('')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

        print('Connection to Redshift cluster {} successfully established! Closing connection again.'.format(config.get('CLUSTER', 'IDENTIFIER')))

        conn.close()

    except Exception as e:
        print('Error connecting to Redshift cluster: {}'.format(e))


def main():

    '''
    FUNCTION:   main
    PURPOSE:    Runs the specified functions to create a Redshift cluster
    INPUT:      None
    OUTPUT:     None
    '''

    clients     = create_clients(config)
    IAM_role    = create_IAM(clients['iam'], config)
    response    = create_VPC_group(clients['ec2'], config)
    response    = create_Redshift(IAM_role, clients['redshift'], clients['ec2'], config)
    response    = check_connectivity(clients['redshift'], config)

if __name__ == '__main__':
    main()
