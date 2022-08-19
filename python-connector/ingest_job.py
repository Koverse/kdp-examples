import os
from kdp_connector import KdpConn

# This example shows you how to use the KDP Python Connector to write data into KDP platform
# from Pandas dataframe sourced from a csv file.


########## variables ##########
# authentication code
email = os.environ.get('EMAIL')
password = os.environ.get('PASSWORD')

# workspace id
workspace_id = os.environ.get('WORKSPACE_ID')

# dataset id
dataset_id = os.environ.get('DATASET_ID')

# host url to KDP platform
kdp_url = os.environ.get('KDP_URL', default='https://api.app.koverse.com')

# list of URLs
url_list = os.environ.get('URL_LIST', default=['https://kdp4.s3-us-east-2.amazonaws.com/test-data/cars.csv'])

# OPTIONAL: When not provided will not verify ssl of request. Which will result in warnings in the log.
# See configuration/configurationUtil.py for more detail about using certificates for ssl.
path_to_ca_file = os.environ.get('PATH_TO_CA_FILE', default='')

##################################################


# Construct kdpConnector
kdp_conn = KdpConn(path_to_ca_file, kdp_url)

authentication_details = kdp_conn.create_authentication_token(email=email,
                                                              password=password,
                                                              workspace_id=workspace_id)

jwt = authentication_details.get("access_token")

job_id = kdp_conn.create_url_ingest_job(workspace_id=workspace_id, dataset_id=dataset_id, url_list=url_list, jwt=jwt)

print('job_id: %s' % job_id)
