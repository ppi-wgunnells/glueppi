"""
script to start dms replication tasks
"""
from glue_etl_util import *

# my role needs permission I had to create the role
# sts_session = boto3.Session()
# sts_client = sts_session.client('sts')
# assumed_role_object = sts_client.assume_role(
#     RoleArn="arn:aws:iam::492436075634:role/druid-admin-role-Role-SET2TUJYEJZT",
#     RoleSessionName="druid-dev-session2"
# )
#
# print(assumed_role_object)

# this on the other hand does work
# sts_gen = boto3.Session()
# client = sts_gen.client('sts')
# response = client.get_session_token(
#     DurationSeconds=900,
#     # SerialNumber='string',
#     # TokenCode='string'
# )
# print(response)


def main(job=False, del_job=False, j_type=None):
    if job:
        create_job(glue_script_path=f"../scripts/dms_replication_{j_type}.py",
                   filename=f"dms_replication_{j_type}.py",
                   role="druid-admin-role-Role-SET2TUJYEJZT",
                   job_name=f"dms_replication_service_{j_type}",
                   bucket="aws-glue-scripts-492436075634-us-west-2",
                   prefix="dms/script/",
                   temp_dir="s3://aws-glue-scripts-492436075634-us-west-2/cms/temp",
                   tags={'cms': 'cms performance'},
                   # connection={'Connections': ['aurora-master']},
                   num_work=2,
                   work_type='G.1X')  # don't forget to change work type if needed
    # def_args='s3://aws-glue-scripts-492436075634-us-west-2/cms/script/pg8000-1.29.4-py3-none-any.whl,'
    #          's3://aws-glue-scripts-492436075634-us-west-2/cms/script/scramp-1.4.4-py3-none-any.whl,'
    #          's3://aws-glue-scripts-492436075634-us-west-2/cms/script/asn1crypto-1.5.1-py2.py3-none-any.whl')
    if del_job:
        delete_job(f'dms_replication_service_{j_type}')


if __name__ == "__main__":
    # main(job=True, j_type='prod')  # create job
    main(del_job=True, j_type='prod')
    pass
