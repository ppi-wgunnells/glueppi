"""
script to start dms replication tasks
"""
from glue_etl_util import *


def main(job=False, del_job=False):
    if job:
        create_job(glue_script_path="../scripts/cms_extract.py",
                   filename="dms_run_dev.py",
                   role="postgres_to_s3",
                   job_name="cms_extract",
                   bucket="aws-glue-scripts-492436075634-us-west-2",
                   prefix="cms/script/",
                   temp_dir="s3://aws-glue-scripts-492436075634-us-west-2/cms/temp",
                   tags={'cms': 'cms performance'},
                   connection={'Connections': ['aurora-master']},
                   num_work=5,
                   def_args='s3://aws-glue-scripts-492436075634-us-west-2/cms/script/pg8000-1.29.4-py3-none-any.whl,'
                            's3://aws-glue-scripts-492436075634-us-west-2/cms/script/scramp-1.4.4-py3-none-any.whl,'
                            's3://aws-glue-scripts-492436075634-us-west-2/cms/script/asn1crypto-1.5.1-py2.py3-none-any.whl')
    if del_job:
        delete_job('cms_extract')
