"""
dms migration
"""
import certifi
import certifi.core
import requests
import requests.adapters
import ast
import boto3
import os
import pg8000.native
import ssl

import ssl
# import psycopg2
from botocore.exceptions import ClientError


def override_where():  # override certifi.core.where()
    return '/tmp/my_cert.pem'


def get_data(data_name):
    """
    Download data
    :param data_name: str() name of file
    :return: return binary object
    """
    s3 = boto3.resource('s3')
    data_download = s3.Object('aws-glue-scripts-492436075634-us-west-2', data_name)
    body = data_download.get()['Body'].read()
    return body


data = get_data(data_name="cms/script/cacert.pem")
with open(override_where(), "wb") as f:
    f.write(data)


# session = boto3.session.Session()
#
# sts_session = boto3.Session()
# sts_client = sts_session.client('sts', region_name='us-west-2')
# cred = sts_client.assume_role(
#     RoleArn="arn:aws:iam::492436075634:role/druid-admin-role-Role-SET2TUJYEJZT",
#     RoleSessionName="druid-dev-session2"
# )
#
# access_key = cred['Credentials']['AccessKeyId']
# secret_key = cred['Credentials']['SecretAccessKey']
# session_key = cred['Credentials']['SessionToken']
#
#
# def get_secret(secret_name, region_name):
#     sec_session = boto3.session.Session(aws_secret_access_key=secret_key,
#                                         aws_access_key_id=access_key,
#                                         aws_session_token=session_key)
#     client = sec_session.client(service_name='secretsmanager', region_name=region_name)
#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name  # full ARN
#         )
#     except Exception as e:
#         print(e)
#     else:
#         if 'SecretString' in get_secret_value_response:
#             return get_secret_value_response['SecretString']
#
# secrets = ast.literal_eval(get_secret("consortium_master", "us-west-2"))
# print(f"secrets: {secrets}")

flag = "psql"  # psql or mysql

ENDPOINT = "consortium-aurora-dev.cluster-cvwrkhjbrnmf.us-west-2.rds.amazonaws.com"  # internal or vpc
# ENDPOINT = "consortium-aurora-dev-writer.cvwrkhjbrnmf.us-west-2.rds.amazonaws.com"
PORT = "5432"  # 5432 or 3306
USER = "iamglue"  # iam user
REGION = "us-west-2"
DBNAME = "postgres"
# os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

# ssl stuff
os.environ["REQUESTS_CA_BUNDLE"] = override_where()
certifi.core.where = override_where
requests.utils.DEFAULT_CA_BUNDLE_PATH = override_where()
requests.adapters.DEFAULT_CA_BUNDLE_PATH = override_where()

ssl_context = ssl.create_default_context()
ssl_context.verify_mode = ssl.CERT_REQUIRED
ssl_context.load_verify_locations('/tmp/my_cert.pem')

# ssl_context = ssl.create_default_context()
# ssl_context.request_ssl = False

# gets the credentials from .aws/credentials
# sec_session = boto3.session.Session(aws_secret_access_key=secret_key,
#                                     aws_access_key_id=access_key,
#                                     aws_session_token=session_key,
#                                     region_name='us-west-2')
sec_session = boto3.session.Session()
client = sec_session.client('rds', region_name='us-west-2')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)
print(token)


def db_connector():
    try:
        if flag == 'mysql':
            print("using mysql")
            conn = mysql.connector.connect(host=ENDPOINT, user=USER, passwd=token, port=PORT, database=DBNAME,
                                           ssl_ca='SSLCERTIFICATE')
        else:
            print("using psql")
            # conn = psycopg2.connect(host=ENDPOINT, user=USER, password=token, port=PORT, database=DBNAME,
            #                         sslrootcert='SSLCERTIFICATE')
            # conn = psycopg2.connect(host=ENDPOINT, user=USER, password=token, port=PORT, database=DBNAME)
            # conn = pg8000.connect(host=ENDPOINT, database=DBNAME, port=PORT, user=USER, password=token)
            conn = pg8000.native.Connection(host=ENDPOINT, database=DBNAME, port=PORT, user=USER,
                                            password=token, ssl_context=ssl_context)

            # cur = conn.cursor()
            # cur.execute("""select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';""")
            query_results = conn.run("""select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';""")

            # query_results = cur.fetchall()
            print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e))

db_connector()
