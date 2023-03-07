"""
script to deploy CMS performance updates
"""
from glue_etl_util import *


def create_con():
    # hard code for now
    connection_name = 'aurora-master'
    conn_type = 'NETWORK'
    ssl_opt = "true"
    subnet_id = "subnet-2b287a6d"
    security_group = ["sg-98dcc7fa", "sg-bcc371c6"]
    print(create_connection(connection_name, conn_type, ssl_opt, subnet_id, security_group))


def main(job=False, del_job=False, net=False, del_net=False):
    if net:
        create_con()
    if del_net:
        print(delete_connection("aurora-master"))
    if job:
        create_job(glue_script_path="../scripts/cms_extract.py",
                   filename="cms_extract.py",
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


if __name__ == "__main__":
    print(get_connection_info('aurora-master'))
    print(list_jobs({'cms': 'cms performance'}))
    # get_job_runs("cms_extract")
    # main(net=True)  # create net first
    # main(job=True)  # create job
    main(del_job=True)
