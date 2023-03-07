"""
dms migration
for pg8000 easier to just upload AWS certs to s3 and tell app where it's at. This works locally
https://lightsail.aws.amazon.com/ls/docs/en_us/articles/amazon-lightsail-download-ssl-certificate-for-managed-database
"""
import boto3
import pg8000.native
import ssl
import logging
import datetime

# logging
msg_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
datetime_format = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=datetime_format)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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


ENDPOINT = "consortium-aurora-dev.cluster-cvwrkhjbrnmf.us-west-2.rds.amazonaws.com"
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


def db_connector():
    """
    SET updated_at=excluded.updated_at, but I think every col but record_id should have it. Master db is king
    :return: None
    """
    debug = False
    try:
        print("Connection to DB: consortium-aurora-dev")
        conn = pg8000.native.Connection(host=ENDPOINT, database=DBNAME, port=PORT, user=USER,
                                        password=token, ssl_context=ssl_context)
        list_table = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
        conn.run("select * from postgres.master_db_auto.tbl_auto_consortium limit 1;")
        field_names = [c['name'] for c in conn.columns]
        field_names.pop(0)  # del record_id
        update_name = []
        for item in field_names:
            val = f'{item}=excluded.{item}'
            update_name.append(val)
        new_name = ','.join(update_name)  # for final
        sql = f"""INSERT into casemanager.afm_tbl_auto_consortium_cms
        SELECT * from master_db_auto.tbl_auto_consortium WHERE true
        ON CONFLICT(record_id) DO UPDATE SET {new_name}"""
        conn.run(sql)
        if debug:
            query_results = conn.run(list_table)
            print(query_results)
        conn.close()
    except Exception as db_error:
        print(f"Database connection failed due to {db_error}")


if __name__ == "__main__":
    ctime = datetime.datetime.utcnow()
    logger.info(f"DB table migration start time: {ctime}")
    db_connector()
    etime = datetime.datetime.utcnow()
    logger.info(f'DB table migration end time: {etime}')
