"""
dms replication service run start job
"""
import ast
import boto3
from botocore.exceptions import ClientError

session = boto3.session.Session()

sts_session = boto3.Session()
sts_client = sts_session.client('sts')
cred = sts_client.assume_role(
    RoleArn="arn:aws:iam::492436075634:role/druid-admin-role-Role-SET2TUJYEJZT",
    RoleSessionName="druid-dev-session2"
)

access_key = cred['Credentials']['AccessKeyId']
secret_key = cred['Credentials']['SecretAccessKey']
session_key = cred['Credentials']['SessionToken']


def get_secret(secret_name, region_name):
    """
    Notes usage:
        secrets = ast.literal_eval(get_secret("druid-rds-will-rds-secrets", "us-west-2"))  # dict()
        print("dbpass: ", secrets["password"], "dbuser: ", secrets["username"])
    :param secret_name: str() of secrets aws object name
    :param region_name: str() of region
    :return:  trans literal string AST to convert or json to load
    """
    sec_session = boto3.session.Session(
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
        aws_session_token=session_key)
    client = sec_session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name  # full ARN
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            return get_secret_value_response['SecretString']

secrets = ast.literal_eval(get_secret("consortium_master", "us-west-2"))
# print(f"secrets: {secrets}")
