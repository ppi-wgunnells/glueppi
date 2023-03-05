"""
dms replication service run start job
"""
import datetime
import boto3

session = boto3.session.Session()


def start_rep_task(name_arn, start_type, cdc_start):
    """
    datetime.datetime(2023, 1, 1),  # YY,MM,DD,HH,min,ss
    possible future integration: CdcStartPosition='string',
                                 CdcStopPosition='string'
    :param name_arn: str() of arn
    :param start_type: str() of start-replication, resume-processing, reload-target
    :param cdc_start:
    :return:
    """
    client = session.client('dms')
    response = client.start_replication_task(
        ReplicationTaskArn=name_arn,
        StartReplicationTaskType=start_type,
        CdcStartTime=cdc_start,
    )
    return response


if __name__ == "__main__":
    task_dev = 'arn:aws:dms:us-west-2:492436075634:task:IUTHHGFLER6GZM7WICKASELOALSLAKIEPGDJ7LQ'
    task_stage = 'arn:aws:dms:us-west-2:492436075634:task:IUTHHGFLER6GZM7WICKASELOALSLAKIEPGDJ7LQ'
    task_prod = 'arn:aws:dms:us-west-2:492436075634:task:FSNMQMRBNFHWZPV4MMVVUDND63NH7QICDOR62IA'
    start_rep_task(task_dev,
                   'start-replication'
                   f'{datetime.datetime.now()}')

