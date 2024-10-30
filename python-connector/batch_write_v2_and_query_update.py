import os
import time
from pprint import pprint
from timeit import default_timer as timer

import pandas as pd
from kdp_api.exceptions import BadRequestException
from kdp_api.models import SecurityLabelInfoParams
from kdp_connector import KdpConn

# This example shows you how to use the KDP Python Connector to write data into KDP platform
# with ABAC using the batch_write_v2 method
# and read the data back from the KDP platform into a dataframe.

# It then also provides examples of lucene query and sql query methods to query the dataset.

# Only authentication details (username and password) are required for this example
# ########## variables ###########################################
# authentication code

email = os.environ.get('EMAIL', default=None)
password = os.environ.get('PASSWORD', default=None)

# Or as an alternative, you can use an API key (email and password not required if an api-key is provided)
api_key = os.environ.get('API_KEY', default=None)

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
input_file = os.environ.get('INPUT_FILE', default='./resources/actorfilms.csv')

#################################################################

# dataset id: This test creates a new dataset, unless a dataset_id is provided
dataset_id = os.environ.get('DATASET_ID', '')

dataset_name = 'Actors in Films'

# number of records in a batch
ingest_batch_size = 1000
read_batch_size = 100000

##################################################

# Read input into dataframe
df = pd.read_csv(input_file)

# Construct kdpConnector
kdp_conn = KdpConn(path_to_ca_file, kdp_url, discard_unknown_keys=True, api_key=api_key)

if (email is not None) and (password is not None):
    authentication_details = kdp_conn.create_and_set_authentication_token(email=email,
                                                                          password=password,
                                                                          workspace_id=workspace_id)

# Get workspace
workspace = kdp_conn.get_workspace(workspace_id=workspace_id)
pprint("Retrieved workspace by id: %s" % workspace.id)

# Get or Create Dataset
if dataset_id != '':
    dataset = kdp_conn.get_dataset(dataset_id=dataset_id)
    pprint("Retrieved dataset by id %s" % dataset.id)
else:
    dataset = kdp_conn.create_dataset(name=dataset_name, workspace_id=workspace.id)
    pprint("Created dataset with name: %s and dataset.id: %s" % (dataset_name, dataset.id))

# Create Security Label Info Params for security label with one field in data
security_label_info_params: SecurityLabelInfoParams = SecurityLabelInfoParams(
    fields=['ActorID'],
    parser_class_name='identity-parser',
    label_handling_policy="ignore",
    replacementString=""
)

# Swap out the above security_label_info_params with the following to apply a custom label to all records
# security_label_info_params = SecurityLabelInfoParams(
#     label="custom_attribute",
#     parser_class_name="apply-label-to-all-records-parser"
# )


# ingest data
partitions_set = kdp_conn.batch_write_v2(dataframe=df, dataset_id=dataset.id, security_label_info_params=security_label_info_params, batch_size=ingest_batch_size)

pprint('File ingest completed with partitions: %s' % partitions_set)

start = timer()

starting_record_id = ''

dataframe = kdp_conn.read_dataset_to_pandas_dataframe(dataset_id=dataset.id,
                                                      starting_record_id=starting_record_id,
                                                      batch_size=read_batch_size)

end = timer()

print('Created pandas dataframe with', dataframe.size, 'elements (number of columns times number of rows) in ',
      end - start, 'seconds')

if dataframe.size < 10000:
    pprint(dataframe)

# Query methods
def sql_query_for_result() -> None:
    try:
        expression = f"""SELECT * from "{dataset.id}" where "ActorID"='nm0000001'"""
        #  SQL query for the dataset, includeInternalFields = true so _koverse_record_id is returned for each record
        #  -- NOTE: User must have attributes assigned to see the data
        record_batch = kdp_conn.post_sql_query(dataset_id=dataset.id, expression=expression, limit=1000, offset=0, include_internal_fields=True)
    except BadRequestException as e:
        pprint(f"Exception encountered: {e}")
        pprint(f"Exception details: {e.body}")
    return record_batch

def update_records(records):
    print("records size: ", len(records))
    # Update the records
    for record in records:
        record['updated'] = True
    # Convert the updated records back to a DataFrame
    updated_df = pd.DataFrame(records)

    # Update the dataset with the updated records
    partitions_set = kdp_conn.update(dataframe=updated_df, dataset_id=dataset.id, security_label_info_params=security_label_info_params, batch_size=ingest_batch_size)
    pprint('File ingest completed with partitions: %s' % partitions_set)
    return records

def lucene_query_for_result() -> None:
    try:
        #  Lucene query for the dataset - NOTE: User must have attributes assigned to see the data
        query_result = kdp_conn.post_lucene_query(dataset_id=dataset.id, expression='nm0000001', limit=100, offset=0)
        pprint(query_result)

    except BadRequestException as e:
        pprint("Exception encountered")
    return

pprint("Waiting 40 seconds for the indexes to be added to the database before querying")
time.sleep(40)
lucene_query_for_result()

pprint("Attempting sql query")
record_batch = sql_query_for_result()

pprint("Attempting to update records")
update_records(record_batch.records)

