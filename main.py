from clean_input_files import clean_input_files
from bq_ops import bq_process
from subprocess import check_call
import os

if __name__ == '__main__':
    clean_input_files()
    bq_process()
    os.chdir('saras_analytics')
    check_call(['dbt','run','--profiles-dir','.dbt'])
    print("completed dbt new staging table in `derived_saras_analytics.product_info`")
