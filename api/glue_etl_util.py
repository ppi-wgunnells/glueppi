"""
Script to create glue etl resource no crawlers
"""

import boto3
import sys
import botocore.exceptions
import botocore.errorfactory
import pprint
import ast

# profile_name='prod'
session = boto3.session.Session()


def open_template(name):
    """
    path to name of file might be different in rio
    ex: 'api/druid_spark_test.py'
    :param name: str() of file name and its path
    :return: str()
    """
    with open(name, 'r') as data:
        template = data.read()
        # print(template)  # debug in rio or console
    return template


def get_database_list(raw=False):
    """
    :param raw: bool() if True print raw list
    :return: list() of database names in glue
    """
    db_name_list = []
    client = session.client('glue')
    db_list = client.get_databases()
    for db in db_list['DatabaseList']:
        if raw:
            print(db)
        else:
            db_name_list.append(db['Name'])
    return db_name_list


def get_tables(database_name):
    client = session.client('glue')
    starting_token = None
    next_page = True
    tables = []
    while next_page:
        paginator = client.get_paginator(operation_name="get_tables")
        response_iterator = paginator.paginate(
            DatabaseName=database_name,
            PaginationConfig={"PageSize": 100, "StartingToken": starting_token},
        )
        for item in response_iterator:
            print(item)
            table_list = item['TableList']
            for table in table_list:
                # print(table['Name'])  # debug only
                tables.append(table['Name'])
            try:
                starting_token = item["NextToken"]
            except KeyError:
                next_page = False
    return tables


def create_database(db_name):
    """
    :param db_name: str() of database name
    :return: None
    """
    client = session.client('glue')
    try:
        response = client.create_database(
            DatabaseInput={
                'Name': db_name,
                'Description': 'boto3 API'
            }
        )
        print(f"Created db: {db_name} -- {response}")
    except botocore.errorfactory.ClientError as error:
        if error.response['Error']['Code'] == 'AlreadyExistsException':
            print("Database already exists: We could delete and rebuild or exit")
        else:
            print(f"Unexpected error: {error}")


def create_table(db_name, table_name, columns, location_s3, parameters, partition=None):
    """
    :param db_name: str() of database name
    :param table_name: str() of table name
    :param columns: list() of dictionary for table schema {'Name': 'id', 'Type': 'string'} UTF8
    :param location_s3: str() s3 location
    :param parameters: dict() of classification and results
    :param partition: list() of dictionary with
            PartitionKey: []
            {'Name': 'string',
            'Type': 'string',
            'Comment': 'string',
            'Parameters': {
            'string': 'string'}
    :return: None
    """
    client = session.client('glue')
    table_input = {
        'Name': table_name,
        'Description': 'boto3 API',
        'StorageDescriptor': {
            'Columns': columns,
            'Location': location_s3,
        },
        'Parameters': parameters
    }
    if partition:
        table_input['PartitionKeys'] = partition
    try:
        response = client.create_table(
            DatabaseName=db_name,
            TableInput=table_input
        )
        print(response)
    except botocore.errorfactory.ClientError as error:
        if error.response['Error']['Code'] == 'AlreadyExistsException':
            print("Table already exists: create a new one or overwrite")
        else:
            print(f"Unexpected error: {error}")


def delete_database(db_name):
    """
    :param db_name: str() of database name
    :return: None
    """
    client = session.client('glue')
    try:
        response = client.delete_database(Name=db_name)
        print(response)
    except Exception as error:
        print(error)


def delete_table(db_name, table_name):
    """
    :param db_name: str() of database name
    :param table_name: str() of table name
    :return: None
    """
    client = session.client('glue')
    try:
        response = client.delete_table(
            DatabaseName=db_name,
            Name=table_name)
        print(response)
    except Exception as error:
        print(error)


def delete_job(job_name):
    client = session.client('glue')
    response = client.delete_job(
        JobName=job_name
    )
    print(response)


def create_job(glue_script_path, filename, role, job_name,
               bucket="mytronbucket",
               prefix="gluetest/script/",
               temp_dir="s3://mytronbucket/gluetest/foo_out",
               connection=None,
               tags=None,
               extra=None,
               lib_path=None,
               num_work=10,
               work_type='G.2X',
               def_args=None):
    """
    --extra-py-files: s3path inside DefaultArguments
    '--additional-python-modules': 'psycopg2-binary'
    psycopg2-binary==2.9.3  # this did not work also
    :param glue_script_path: str() location for script
    :param filename: str() etl script file name to upload
    :param role: str() of role to use
    :param job_name: str() of job name for task
    :param bucket: str() of bucket location for etl script
    :param prefix: str() of prefix location for etl script without leading slash
    :param temp_dir: str() of output
    :param connection: dict() of connection setting {'Connections': ['string']}
    :param tags: dict() of tags key val pair
    :param extra: dict() of extra strings
    :param lib_path: str() of binary object
    :param num_work: int() of worker nodes
    :param work_type: str() of worker type default 'G.2X'
    :return: None
    """
    s3_client = session.resource('s3')  # put some data
    client = session.client('glue')  # start some jobs
    print(prefix)
    if lib_path:
        print("debug")
        print(lib_path)
        print(f"{bucket}{prefix}{lib_path}")
        s3_lib = s3_client.Object(bucket, prefix + lib_path.replace("./", ""))
        lib_response = s3_lib.put(Body=open(lib_path, "rb"))
        print(lib_response)
    s3_data = s3_client.Object(bucket, prefix + filename)
    response = s3_data.put(Body=open(glue_script_path, "rb"))
    print(response)
    # 'AllocatedCapacity': 20,
    # MaxCapacity': 100,  # do not set if NumberofWorks and WorkerType, this is done auto
    # NumberOfWorkers should not exceed 10, 3 might be sweet spot
    job = {
           'NumberOfWorkers': num_work,
           'WorkerType': work_type,
           'Command': {
               'Name': 'glueetl',
               'ScriptLocation': f's3://{bucket}/{prefix}{filename}',
               'PythonVersion': '3'
           },
           'DefaultArguments': {'--TempDir': temp_dir,
                                '--job-bookmark-option': 'job-bookmark-disable',
                                },
           'ExecutionProperty': {'MaxConcurrentRuns': 1},
           'MaxRetries': 0,
           'GlueVersion': '4.0',
           'Name': job_name,
           'Role': role,
           }
    if def_args:  # long csv string
        job['DefaultArguments']['--extra-py-files'] = def_args
    if connection:
        job["Connections"] = connection
    if tags:
        job["Tags"] = tags
    if extra:
        job["DefaultArguments"] = extra
    job_create_response = client.create_job(**job)
    print("job create: ====")
    print(job_create_response)
    job_run_response = client.start_job_run(JobName=job_name)
    print("job run: ====")
    print(job_run_response)


def list_jobs(tag):
    """
    :param tag: dict() of key value tag {'foo': 'bar'}
    :return: dict() of response
    """
    client = session.client('glue')
    response = client.list_jobs(
        MaxResults=123,
        Tags=tag
    )
    return response


def get_job(job_name):
    """
    :param job_name: str() of job name
    :return: dict()
    """
    client = session.client('glue')
    response = client.get_job(
        JobName=job_name
    )
    return response


def get_connection_info(conn_name):
    """
    :param conn_name: str() of connection name
    :return: None
    """
    client = session.client('glue')
    try:
        response = client.get_connection(Name=conn_name)
        return response
    except ClientError as e:
        raise Exception(f"boto3 client error in get_connection_info: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error in get_connection_info: {e}")


def create_connection(connection_name, conn_type, ssl_opt, subnet_id, security_group):
    """
    :param connection_name: str() of connection name
    :param conn_type: str() of connection type JDBC, KAFKA, MONGODB, NETWORK, MARKETPLACE, CUSTOM
    :param ssl_opt: str() of false or true lowercase
    :param subnet_id: str() of subnet id
    :param security_group: list() of security groups
    :return: dict() of response Metadata
    """
    client = session.client('glue')
    try:
        response = client.create_connection(
            ConnectionInput={
                'Name': connection_name,
                'ConnectionType': conn_type,
                'ConnectionProperties': {
                    'JDBC_ENFORCE_SSL': ssl_opt
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': subnet_id,
                    'SecurityGroupIdList': security_group,
                    'AvailabilityZone': 'us-west-2c'
                }
            }
        )
        return response
    except botocore.errorfactory.ClientError as error:
        if error.response['Error']['Code'] == 'AlreadyExistsException':
            print("connection already exists: create a new one or delete")
        else:
            print(f"Unexpected error: {error}")


def delete_connection(connection_name):
    """
    :param connection_name: str() of connection name
    :return: None
    """
    client = session.client('glue')
    try:
        response = client.delete_connection(
            ConnectionName=connection_name
        )
        return response
    except Exception as error:
        print(error)


def get_job_runs(job_name):
    client = session.client('glue')
    response = client.get_job_run(
        JobName=job_name,
        RunId='string',
        PredecessorsIncluded=True|False
    )
    print(response)

# def get_secret(secret_name, region_name):
#     """
#     Notes usage:
#         secrets = ast.literal_eval(get_secret("druid-rds-will-rds-secrets", "us-west-2"))  # dict()
#         print("dbpass: ", secrets["password"], "dbuser: ", secrets["username"])
#     :param secret_name: str() of secrets aws object name
#     :param region_name: str() of region
#     :return:  trans literal string AST to convert or json to load
#     """
#     # session = boto3.session.Session()  # already global
#     client = session.client(
#         service_name='secretsmanager',
#         region_name=region_name
#     )
#     get_secret_value_response = client.get_secret_value(
#         SecretId=secret_name
#     )
#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
#     except ClientError as e:
#         if e.response['Error']['Code'] == 'DecryptionFailureException':
#             raise e
#         elif e.response['Error']['Code'] == 'InternalServiceErrorException':
#             raise e
#         elif e.response['Error']['Code'] == 'InvalidParameterException':
#             raise e
#         elif e.response['Error']['Code'] == 'InvalidRequestException':
#             raise e
#         elif e.response['Error']['Code'] == 'ResourceNotFoundException':
#             raise e
#     else:
#         if 'SecretString' in get_secret_value_response:
#             secret = get_secret_value_response['SecretString']
#         else:
#             decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
#     return secret
