import os
import pandas as pd

from pprint import pprint

from timeit import default_timer as timer

import time

from kdp_api.exceptions import BadRequestException
from kdp_api.models import SecurityLabelInfoParams
import kdp_api
from kdp_api import Query
from kdp_api.api import read_and_query_api

from kdp_connector import KdpConn

# This example shows you how to use the KDP Python Connector to write data into KDP platform
# with ABAC using the batch_write_v2 method
# and read the data back from the KDP platform into a dataframe.

# Only authentication details (username and password) are required for this example
# ########## variables ###########################################
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
kdp_conn = KdpConn(path_to_ca_file, kdp_url, discard_unknown_keys=True)

authentication_details = kdp_conn.create_authentication_token(email=email,
                                                              password=password,
                                                              workspace_id=workspace_id)

jwt = authentication_details.access_token

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
partitions_set = kdp_conn.batch_write_v2(df, dataset.id, jwt, security_label_info_params, ingest_batch_size)

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

# Query methods
def post_sql_query(dataset_id: str, expression: str, limit: int = 5, offset: int = 0):
    """This method will be used to query data in KDP datasets using the lucene syntax

        :param Configuration config: Connection configuration
        :param str dataset_id: ID of the KDP dataset where the data will queried
        :param str expression: Lucene style query expression ex. name: John

        :returns: Records matching query expression

        :rtype: RecordBatch
    """
    config = kdp_conn.create_configuration(jwt)
    with kdp_api.ApiClient(config) as api_client:
        api_instance = read_and_query_api.ReadAndQueryApi(api_client)

        query = Query(datasetId=dataset_id, expression=expression, limit=limit, offset=offset)

        return api_instance.post_query(query=query)


def sql_query_for_result() -> None:
    try:
        expression = "SELECT * from \"%s\" where \"ActorID\"='nm0000001'" % dataset.id
        #  Lucene query for the dataset -- NOTE: User must have attributes assigned to see the data
        query_result = post_sql_query(dataset_id=dataset.id, expression=expression, limit=100, offset=0)
        pprint(query_result)

    except BadRequestException as e:
        pprint("Exception encountered")
    return


def lucene_query_for_result() -> None:
    try:
        #  Lucene query for the dataset - NOTE: User must have attributes assigned to see the data
        query_result = kdp_conn.post_lucene_query(dataset_id=dataset.id, jwt=jwt, expression='nm0000001', limit=100, offset=0)
        pprint(query_result)

    except BadRequestException as e:
        pprint("Exception encountered")
    return

pprint("Waiting 40 seconds for the indexes to be added to the database before querying")
time.sleep(40)
lucene_query_for_result()

pprint("Attempting sql query")
sql_query_for_result()
