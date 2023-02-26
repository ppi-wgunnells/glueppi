"""
copy CBM data from Lizard-dev to tag sourcing RDS
"""

import ast
import logging
import time
import datetime
import json
import pymysql.cursors
import boto3
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from tool.tools import write_cert, tag_environment_override, get_secret
from tool.hubble_tool import HubbleClient
# logging
msg_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
datetime_format = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=datetime_format)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

write_cert()  # certificate needs to be written before doing anything with environment or apple_connect
tag_environment_override()  # environment needs to be set before initialization of tag_interface and keycloak

# creds, context, UDF and boto3 sessions
DBURL = "vpce-01734e436d5884b51-tibw8ii4.vpce-svc-030fa15c06134e2b5.us-west-2.vpce.amazonaws.com"  # cross act
DBUSER = "rdsatappleadmin"
secrets = ast.literal_eval(get_secret("tag_credentials", "us-west-2"))  # dict()
TOKEN = secrets["Lizard_Prod_RDS"]
DBNAME = "SCBProd"
spark = SparkSession.builder \
    .config("driver", "com.mysql.jdbc.Driver") \
    .getOrCreate()
source_bucket = "08b9f0a5-1208-4bf1-92ea-7bd24d329e01"
source_prefix = ""  # manual uploads
udf_dst_prefix = "automate/"  # manual uploads must have 'name/'
internal = 1  # internal 0, public 1
cbm_session = boto3.Session()
sts_session = boto3.Session()
sts_client = sts_session.client('sts')
assumed_role_object = sts_client.assume_role(
    RoleArn="arn:aws:iam::133999959307:role/druid-developer-role-Role-YBHKPRF2J8DH",
    RoleSessionName="druid-dev-session2"
)

# HubbleClient
url = "hubble-auth-publish-pie-prod.apple.com"
hubble_token = secrets['Hubble_token']
ha = HubbleClient(app_name="a9ops-analytics", dc='us-west-1a', partition='tag-copy', hostname='GlueETL',
                  hubble_url=url, port="443", token=hubble_token)

accum_total = spark.sparkContext.accumulator(0)
accum_missing = spark.sparkContext.accumulator(0)
accum_bytes = spark.sparkContext.accumulator(0)
accum_attempt = spark.sparkContext.accumulator(0)


def reports(exec_time):
    if accum_attempt.value > 0:
        delta = {'sp': accum_total.value, 'tt': accum_total.value, 'c': 1, 'a': accum_total.value,
                 'm': accum_total.value, 'x': accum_total.value, 'pf': 60}
        ha.put(instance="some_instance", data=delta, a_type="CBM", name="CBM_TAG_COPY", kpi="CBM_copy_files")
        delta = {'sp': accum_missing.value, 'tt': accum_missing.value, 'c': 1, 'a': accum_missing.value,
                 'm': accum_missing.value, 'x': accum_missing.value, 'pf': 60}
        ha.put(instance="some_instance", data=delta, a_type="CBM", name="CBM_TAG_COPY", kpi="CBM_copy_fail")
        delta = {'sp': accum_bytes.value, 'tt': accum_bytes.value, 'c': 1, 'a': accum_bytes.value,
                 'm': accum_bytes.value, 'x': accum_bytes.value, 'pf': 60}
        ha.put(instance="some_instance", data=delta, a_type="CBM", name="CBM_TAG_COPY", kpi="CBM_TAG_bytes")
        delta = {'sp': exec_time, 'tt': exec_time, 'c': 1, 'a': exec_time, 'm': exec_time, 'x': exec_time, 'pf': 60}
        ha.put(instance="some_instance", data=delta, a_type="CBM", name="CBM_TAG_COPY", kpi="CBM_Time")


def exec_query(sql, name=DBNAME, user=DBUSER, d_pass=TOKEN, host=DBURL, port=3306):
    """
    sql strings, and parameters
    """
    conn = pymysql.connect(database=name, user=user, password=d_pass, host=host, port=port)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()


@udf()
def conversion(did, item):
    """
    :param did: str() of id
    :param item: str() of video_file
    :return str() of http_tag_url with new url
    """
    import socket
    import logging
    import io
    accum_attempt.add(1)
    msg_format_udf = '%(asctime)s %(levelname)s %(name)s: %(message)s'
    datetime_format_udf = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(format=msg_format_udf, datefmt=datetime_format_udf)
    logger_udf = logging.getLogger(__name__)
    logger_udf.setLevel(logging.INFO)
    hostname = socket.gethostname()
    ip_addr = socket.gethostbyname(hostname)
    logger_udf.info(f"Your Cluster Name: is:{hostname}, address: {ip_addr}")
    # lizard CBM
    cbm_credentials = assumed_role_object['Credentials']
    cbm_session_config = {
        'aws_access_key_id': cbm_credentials['AccessKeyId'],
        'aws_secret_access_key': cbm_credentials['SecretAccessKey'],
        'aws_session_token': cbm_credentials['SessionToken'],
    }
    s3_cbm = cbm_session.resource('s3', **cbm_session_config)
    # Tag
    write_cert()  # certificate needs to be written before doing anything with environment or apple_connect
    tag_environment_override()  # environment needs to be set before initialization of tag_interface and keycloak
    from tag_tool.tag_interface import TagTool  # DO NOT MOVE TO TOP requires cert and env settings for tag
    tag_session = boto3.Session()
    get_cred = ""
    creds = str
    cred = dict
    try:
        get_cred = TagTool()
        creds = get_cred.credentials(space="SIRICBM").text
        cred = json.loads(creds)
        logger_udf.info(f"LOG: {creds}")
        target = cred['S3UploadTargets'][internal]  # internal 0, public 1
        tag_session_config = {
            'aws_access_key_id': cred['Credentials']['AccessKeyId'],
            'aws_secret_access_key': cred['Credentials']['SecretAccessKey'],
            'aws_session_token': cred['Credentials']['SessionToken'],
        }
        s3_tag = tag_session.resource('s3', **tag_session_config)
        filter_name = "data-collection"
        file_fix = item.split(filter_name)
        s3_path = f"s3a://{source_bucket}/{filter_name}{file_fix[1]}"
        fix_dst_key = f"{target['Prefix']}{udf_dst_prefix}{filter_name}{file_fix[1]}"
        logger_udf.info(f"LOG: {fix_dst_key}")
        s3_cbm_file = s3_cbm.Object(source_bucket, f"{filter_name}{file_fix[1]}")  # src bucket and key
        byte_size = s3_cbm_file.content_length
        try:
            buff = io.BytesIO()
            get_body = s3_cbm_file.get()['Body'].read()
            tag_data = s3_tag.Object(target['Bucket'], fix_dst_key)
            buff.write(get_body)
            response = tag_data.put(Body=buff.getvalue())  # getvalue returns entire contents regardless of stream pos
            logger_udf.info(f"LOG: {str(response)}")
            file_key = f"{target['DownloadUrl']}/{target['Prefix']}{udf_dst_prefix}{filter_name}{file_fix[1]}"
            sql = f"UPDATE automated_trial SET http_tag_url = '{file_key}' WHERE id = '{did}'"
            logger_udf.info(f"LOG: sql {sql}")
            exec_query(sql)
            accum_total.add(1)
            accum_bytes.add(byte_size)
            return
        except Exception as error:
            logger_udf.info(f"Error: {error}, Missing Key or prefix problem: {s3_path}")
            return
    except Exception as tag_error:
        logger_udf.info(f"TAG EXCEPTION!!! {tag_error}")
        logger_udf.info(f"CREDS: {creds}")
        logger_udf.info(f"CREDS DICT: {str(cred)}")
        accum_missing.add(1)
        return


def extract_rds_assets(utc_date, prev_day):
    """
    :param utc_date: str() YYYY-MM-DD
    :param prev_day: str() previous day YYYY-MM-DD
    :return: None
    """
    start_time = time.time()
    logger.info(f"utc date: {utc_date}")
    logger.info(f"utc prev_day: {prev_day}")
    current_int_date = int(utc_date.timestamp())
    prev_int_date = int(prev_day.timestamp())
    sql_query = f"""select *, timestampdiff(SECOND, '1970-01-01 00:00:00', created_date) as int_date
            from automated_trial where 
            DATE(created_date) between '{prev_day.date()}' AND '{utc_date.date()}' AND
            http_tag_url is null"""
    data_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://" + DBURL + ":3306/SCBProd") \
        .option("dbtable", f"({sql_query}) AS t") \
        .option("numPartitions", 20) \
        .option("partitionColumn", "int_date") \
        .option("lowerBound", prev_int_date) \
        .option("upperBound", current_int_date) \
        .option("user", DBUSER) \
        .option("password", TOKEN) \
        .load()
    data_df = data_df.repartition(20).persist()
    print(f"count: {data_df.count()}")
    df_transform = data_df.select(data_df.video_file, data_df.created_date,
                                  conversion(data_df.id, data_df.video_file).alias('http_tag_url'))
    df_transform = df_transform.persist()
    print('record count processed: ' + str(df_transform.count()))
    execute_time = time.time() - start_time
    reports(execute_time)


if __name__ == "__main__":
    ctime = datetime.datetime.utcnow()
    logger.info(f"let's log time: {ctime}")
    utc_date_now = ctime
    prev_date = utc_date_now - timedelta(days=1)
    extract_rds_assets(utc_date=utc_date_now, prev_day=prev_date)
