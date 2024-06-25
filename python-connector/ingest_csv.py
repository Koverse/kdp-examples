from pprint import pprint

import os
import pandas as pd

from kdp_connector import KdpConn

# This example shows you how to use the KDP Python Connector to write data into KDP platform
# from Pandas dataframe sourced from a csv file.


# ######### variables ##########
# authentication code
email = os.environ.get('EMAIL')
password = os.environ.get('PASSWORD')

# workspace id
workspace_id = os.environ.get('WORKSPACE_ID')

# dataset id
dataset_id = os.environ.get('DATASET_ID')

# host url to KDP platform
kdp_url = os.environ.get('KDP_URL', default='https://api.app.koverse.com')

# OPTIONAL: number of records to read per batch when writing to dataframe, default:1000;
batch_size = os.environ.get('BATCH_SIZE', default=100000)

# OPTIONAL: When not provided will not verify ssl of request. Which will result in warnings in the log.
# See configuration/configurationUtil.py for more detail about using certificates for ssl.
path_to_ca_file = os.environ.get('PATH_TO_CA_FILE', default='')

# csv file location
input_file = os.environ.get('INPUT_FILE', default='resources/actorfilms.csv')

##################################################

# Read input into dataframe
df = pd.read_csv(input_file)

# Construct kdpConnector
kdp_conn = KdpConn(path_to_ca_file, kdp_url)

authentication_details = kdp_conn.create_authentication_token(email=email,
                                                              password=password,
                                                              workspace_id=workspace_id)

jwt = authentication_details.access_token

# ingest data
partitions_set = kdp_conn.batch_write(df, dataset_id, jwt, batch_size)

pprint('partitions: %s' % partitions_set)
