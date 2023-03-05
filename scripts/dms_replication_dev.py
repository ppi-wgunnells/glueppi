"""
dms replication service run start job
"""
import logging
import datetime
import boto3

session = boto3.session.Session()


# logging
msg_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
datetime_format = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=datetime_format)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def start_rep_task(name_arn, start_type):
    """
    datetime.datetime(2023, 1, 1),  # YY,MM,DD,HH,min,ss
    possible future integration: CdcStartPosition='string',
                                 CdcStopPosition='string'
    :param name_arn: str() of arn
    :param start_type: str() of start-replication, resume-processing, reload-target
    :return:
    """
    client = session.client('dms')
    response = client.start_replication_task(
        ReplicationTaskArn=name_arn,
        StartReplicationTaskType=start_type,
    )
    return response


if __name__ == "__main__":
    ctime = datetime.datetime.utcnow()
    logger.info(f"let's log time: {ctime}")
    task_dev = 'arn:aws:dms:us-west-2:492436075634:task:TCB6O2CNRUOHN3EIFPZLT2GNGZK5G2LE3KYIONQ'
    task_stage = 'arn:aws:dms:us-west-2:492436075634:task:FSNMQMRBNFHWZPV4MMVVUDND63NH7QICDOR62IA'
    task_prod = 'arn:aws:dms:us-west-2:492436075634:task:IUTHHGFLER6GZM7WICKASELOALSLAKIEPGDJ7LQ'
    start_rep_task(task_dev, 'start-replication')
    etime = datetime.datetime.utcnow()
    logger.info(f'end time: {etime}')

