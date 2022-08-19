import os

from pprint import pprint

from timeit import default_timer as timer

from kdp_connector import KdpConn

# This example shows you how to use the KDP Python Connector to read data from KDP dataset into
# a Pandas Dataframe.

########## variables ##########
# authentication code
email = os.environ.get('EMAIL')
password = os.environ.get('PASSWORD')

# workspace id
workspace_id = os.environ.get('WORKSPACE_ID')

# dataset id
dataset_id = os.environ.get('DATASET_ID')

# OPTIONAL: host url to KDP platform
kdp_url = os.environ.get('KDP_URL', default='https://api.app.koverse.com')

# OPTIONAL: number of records to read per batch when writing to dataframe, default:1000;
batch_size = os.environ.get('BATCH_SIZE', default=100000)

# OPTIONAL: When provided the dataset will be starting from the record following the starting_record_id
starting_record_id = os.environ.get('STARTING_RECORD_ID', default='')

# OPTIONAL: When not provided will not verify ssl of request. Which will result in warnings in the log.
# See configuration/configurationUtil.py for more detail about using certificates for ssl.
path_to_ca_file = os.environ.get('PATH_TO_CA_FILE', default='')

#####################################################


start = timer()

kdp_conn = KdpConn(path_to_ca_file, kdp_url)

authentication_details = kdp_conn.create_authentication_token(email=email,
                                                              password=password,
                                                              workspace_id=workspace_id)

jwt = authentication_details.get("access_token")

dataframe = kdp_conn.read_dataset_to_pandas_dataframe(dataset_id=dataset_id,
                                                      jwt=jwt,
                                                      starting_record_id=starting_record_id,
                                                      batch_size=batch_size)

end = timer()

print('Created pandas dataframe with', dataframe.size, 'elements (number of columns times number of rows) in ',
      end - start, 'seconds')

if dataframe.size < 5000:
    pprint(dataframe)
