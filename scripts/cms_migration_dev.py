"""
dms migration
for pg8000 easier to just upload AWS certs to s3 and tell app where it's at. This works locally
https://lightsail.aws.amazon.com/ls/docs/en_us/articles/amazon-lightsail-download-ssl-certificate-for-managed-database
"""
import boto3
import pg8000.native
import ssl

# global
local = False
client_kwargs = {}

def override_where():  # override certifi.core.where()
    return '/tmp/my_cert.pem'


def get_data(data_name):
    s3 = boto3.resource('s3')
    data_download = s3.Object('aws-glue-scripts-492436075634-us-west-2', data_name)
    body = data_download.get()['Body'].read()
    return body


data = get_data(data_name="cms/script/cacert.pem")
with open(override_where(), "wb") as f:
    f.write(data)

if local:
    session = boto3.session.Session()
    sts_session = boto3.Session()
    sts_client = sts_session.client('sts', region_name='us-west-2')
    cred = sts_client.assume_role(
        RoleArn="arn:aws:iam::492436075634:role/druid-admin-role-Role-SET2TUJYEJZT",
        RoleSessionName="druid-dev-session2")
    client_kwargs['aws_access_key_id'] = cred['Credentials']['AccessKeyId']
    client_kwargs['aws_secret_access_key'] = cred['Credentials']['SecretAccessKey']
    client_kwargs['aws_session_token'] = cred['Credentials']['SessionToken']


ENDPOINT = "consortium-aurora-dev.cluster-cvwrkhjbrnmf.us-west-2.rds.amazonaws.com"  # internal or vpc
# ENDPOINT = "consortium-aurora-dev-writer.cvwrkhjbrnmf.us-west-2.rds.amazonaws.com"
PORT = "5432"  # 5432 or 3306
USER = "iamglue"  # iam user
REGION = "us-west-2"
DBNAME = "postgres"


# ssl stuff
ssl_context = ssl.create_default_context()
ssl_context.verify_mode = ssl.CERT_REQUIRED
ssl_context.load_verify_locations('/tmp/my_cert.pem')

sec_session = boto3.session.Session(**client_kwargs)
client = sec_session.client('rds', region_name='us-west-2')
token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)
print(token)


def db_connector():
    try:
        print("using psql")
        conn = pg8000.native.Connection(host=ENDPOINT, database=DBNAME, port=PORT, user=USER,
                                        password=token, ssl_context=ssl_context)
        query_results = conn.run("""select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';""")
        print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e))


db_connector()
