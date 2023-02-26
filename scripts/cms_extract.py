"""
CMS extract data from master_db_auto and insert into casemanager
"""
import datetime
import logging
import boto3

aws = True
local = False
if aws:
    import pg8000
if local:
    import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


session = boto3.session.Session()
stage = 'dev'
DBNAME = "postgres"
SCHEMA_master = "master_db_auto"  # starts here in production
afm = "afm_"
cms = '_cms'
SCHEMA_cms = "casemanager"
Schema_pre = "will_"  # a million
Schema_pre1 = "will1_"  # 1000
Schema_pre2 = "will2_"  # 100
Schema_pre3 = "will3_"  # 200
Schema_pre10 = "will10_"  # 10
Schema_pre20 = "will20_"  # 20

# logging
msg_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
datetime_format = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=datetime_format)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ssm_client = boto3.client('ssm')

spark = SparkSession \
    .builder \
    .appName("CMS Performance copy") \
    .config("driver", "com.postgresql.jdbc.Driver") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .config('spark.driver.memory', '15g') \
    .getOrCreate()


def get_parameter(client, name, with_decryption=True):
    response = client.get_parameter(Name=name, WithDecryption=with_decryption)
    return response['Parameter']['Value']


# username = get_parameter(ssm_client, f'/apps/aurora_postgres_{stage}/username', with_decryption=True)
# password = get_parameter(ssm_client, f'/apps/aurora_postgres_{stage}/password', with_decryption=True)
# hostname = get_parameter(ssm_client, f'/apps/aurora_postgres_{stage}/hostname', with_decryption=True)
username = 'postgres'
password = 'Rit7rnYZ9zBCJ6hj'
hostname = 'consortium-aurora-dev.cluster-cvwrkhjbrnmf.us-west-2.rds.amazonaws.com'
print(username)
print(password)
print(hostname)
# write to file on s3


def db_execute(sql, user, passwd, host, db_name='postgres', search=False, bypass=False, split=None):
    """
    :param sql: str() of sql query
    :param user: str() of username
    :param passwd: str() of password
    :param host: str() of host
    :param db_name: str() of database table name
    :param search: bool() of search, we want to return fetchone()
    :param bypass: bool() of bypass return all tuple values
    :param split:
    :return:
    """
    if aws:
        conn = pg8000.connect(host=host, database=db_name, port='5432', user=user, password=passwd)
    if local:
        conn = psycopg2.connect(host=host, dbname=db_name, port='5432', user=user, password=passwd)
    cur = conn.cursor()
    if split:
        # print(cur.mogrify(sql, split)) # .decode('utf8'))
        cur.execute(sql, split)
    cur.execute(sql)
    if bypass:
        conn.commit()
        return cur
    record = cur.fetchone()
    if search:
        return record
    conn.commit()


q = f"""select column_name from information_schema.columns 
where table_schema = '{SCHEMA_cms}' and table_name='{Schema_pre20}{afm}tbl_auto_consortium{cms}'"""
data = db_execute(q, username, password, hostname, bypass=True)
column_names = [row[0] for row in data]



@udf
def conversion(did, ddate, update):
    val_list = []
    info = ''
    print("Inside Conversion")
    print(f"record_id: {did}, created_at: {ddate}, updated_at: {update}")
    # check if exists in small table
    sql = f"""select record_id, created_at, updated_at
    FROM {SCHEMA_cms}.{Schema_pre1}{afm}tbl_auto_consortium{cms}
    WHERE record_id = '{did}' AND created_at = '{ddate}'"""
    record = db_execute(sql, username, password, hostname, search=True)
    if record:
        # print(f"Found record: {record[1]}")
        # info = f"Found record: {record[1]}"  # move down
        if record[1] == ddate and record[2] == update:
            pass
    else:
        print(f"cool we have new records: {record}")
        info = f"cool we have new records {did}"  # only did is available
        # insert into small from big
        query = f"""insert into {SCHEMA_cms}.{Schema_pre1}{afm}tbl_auto_consortium{cms} select * 
        from {SCHEMA_cms}.{Schema_pre}{afm}tbl_auto_consortium{cms} where record_id = '{did}'"""
        print(query)
        db_execute(query, username, password, hostname, bypass=True)
    return info


def extract_master_db_auto():
    # big or master table
    sql_query = f"""SELECT record_id, created_at, updated_at
    FROM {SCHEMA_cms}.{Schema_pre}{afm}tbl_auto_consortium{cms}"""
    print(sql_query)
    data_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{hostname}:5432/{DBNAME}") \
        .option("dbtable", f"({sql_query}) AS t") \
        .option("numPartitions", 5) \
        .option("partitionColumn", "record_id") \
        .option("lowerBound", 0) \
        .option("upperBound", 10) \
        .option("user", username) \
        .option("password", password) \
        .load()  # .limit(2)
    # data_df = data_df.repartition(5).persist()  # this could mess things up
    data_df.persist()
    print(f"count: {data_df.count()}")
    df_transform = data_df.select(data_df.record_id, conversion(data_df.record_id, data_df.created_at, data_df.updated_at).alias("foo"))
    df_transform = df_transform.persist()
    df_transform.show(200, truncate=False)
    print('record count processed: ' + str(df_transform.count()))


if __name__ == "__main__":
    ctime = datetime.datetime.utcnow()
    logger.info(f"let's log time: {ctime}")
    extract_master_db_auto()
    etime = datetime.datetime.utcnow()
    logger.info(f'end time: {etime}')

