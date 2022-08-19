import os
import pandas as pd

from pprint import pprint

from timeit import default_timer as timer

from kdp_connector import KdpConn

# This example shows you how to use the KDP Python Connector to write data into KDP platform
# from Pandas dataframe sourced from a csv file.
# Also shows how to read the data back from the KDP platform into a dataframe.

# Only authentication details (username and password) are required for this example
########### variables ###########################################
########## variables ##########
# authentication code
email = os.environ.get('EMAIL')
password = os.environ.get('PASSWORD')

# workspace id
workspace_id = os.environ.get('WORKSPACE_ID')

# host url to KDP platform
kdp_url = os.environ.get('KDP_URL', default='https://api.app.koverse.com')

# OPTIONAL: number of records to read per batch when writing to dataframe, default:1000;
batch_size = os.environ.get('BATCH_SIZE', default=100000)

# OPTIONAL: When provided the dataset will be starting from the record following the starting_record_id
starting_record_id = os.environ.get('STARTING_RECORD_ID', default='')

# OPTIONAL: When not provided will not verify ssl of request. Which will result in warnings in the log.
# See configuration/configurationUtil.py for more detail about using certificates for ssl.
path_to_ca_file = os.environ.get('PATH_TO_CA_FILE', default='')

# csv file location
input_file = os.environ.get('INPUT_FILE', default='../datafiles/actorfilms.csv')

#################################################################

# dataset id: This test creates a new dataset instead of using DATASET_ID
dataset_id = ''

dataset_name = 'Actors in Films'

# number of records in a batch
ingest_batch_size = 1000
read_batch_size = 100000

##################################################

# Read input into dataframe
df = pd.read_csv(input_file)

# Construct kdpConnector
kdp_conn = KdpConn(path_to_ca_file, kdp_url, discard_unknown_keys=True)

authentication_details = kdp_conn.create_authentication_token(email=email,
                                                              password=password,
                                                              workspace_id=workspace_id)

jwt = authentication_details.get("access_token")

# Get workspace
workspace = kdp_conn.get_workspace(workspace_id, jwt)
pprint("Retrieved workspace by id: %s" % workspace.id)

# Get or Create Dataset
if dataset_id != '':
    dataset = kdp_conn.get_dataset(dataset_id, jwt)
    pprint("Retrieved dataset by id %s" % dataset.id)
else:
    dataset = kdp_conn.create_dataset(name=dataset_name, workspace_id=workspace.id, jwt=jwt)
    pprint("Created dataset with name: %s and dataset.id: %s" % (dataset_name, dataset.id))

# ingest data
partitions_set = kdp_conn.batch_write(df, dataset.id, jwt, ingest_batch_size)

pprint('File ingest completed with partitions: %s' % partitions_set)

start = timer()

starting_record_id = ''

dataframe = kdp_conn.read_dataset_to_pandas_dataframe(dataset_id=dataset.id,
                                                      jwt=jwt,
                                                      starting_record_id=starting_record_id,
                                                      batch_size=read_batch_size)

end = timer()

print('Created pandas dataframe with', dataframe.size, 'elements (number of columns times number of rows) in ',
      end - start, 'seconds')

if dataframe.size < 10000:
    pprint(dataframe)
