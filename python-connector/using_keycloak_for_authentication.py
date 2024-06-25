import os

from pprint import pprint

from timeit import default_timer as timer

from kdp_connector import KdpConn

# This example shows you how to use the KDP Python Connector to authenticate as a keycloak user using keycloak oauth provider
# In the example we will read data from a dataset using a keycloak user's credentials
# Prerequisites for the keycloak authentication...
#   A koverse environment that has keycloak oauth configured.
#   A keycloak user in that environment that can login to koverse UI using keycloak credentials

# Only authentication details (keycloak client id and secret, keycloak user username and password, workspace, and host) are required for this example
# keycloak client variables
keycloak_host = os.environ.get('KEYCLOAK_HOST')
keycloak_realm = os.environ.get('KEYCLOAK_REALM')
keycloak_client_id = os.environ.get('KEYCLOAK_CLIENT_ID')
keycloak_client_secret = os.environ.get('KEYCLOAK_CLIENT_SECRET')

# keycloak user variables
keycloak_username = os.environ.get("KEYCLOAK_USERNAME")
keycloak_password = os.environ.get("KEYCLOAK_PASSWORD")

# workspace id
workspace_id = os.environ.get('WORKSPACE_ID')

# dataset id
dataset_id = os.environ.get('DATASET_ID')

# host url to KDP platform
kdp_url = os.environ.get('KDP_URL', default='https://api.app.koverse.com')


# OPTIONAL: number of records to read per batch when writing to dataframe, default:1000;
batch_size = os.environ.get('BATCH_SIZE', default=100000)

# OPTIONAL: When provided the dataset will be starting from the record following the starting_record_id
starting_record_id = os.environ.get('STARTING_RECORD_ID', default='')

# OPTIONAL: When not provided will not verify ssl of request. Which will result in warnings in the log.
# See configuration/configurationUtil.py for more detail about using certificates for ssl.
path_to_ca_file = os.environ.get('PATH_TO_CA_FILE', default='')

# OPTIONAL: csv file location
input_file = os.environ.get('INPUT_FILE', default='./resources/actorfilms.csv')

# Read input into dataframe
start = timer()

kdp_conn = KdpConn(path_to_ca_file, kdp_url)

authentication_details = kdp_conn.create_keycloak_authentication_token(username=keycloak_username,
                                                                       realm=keycloak_realm,
                                                                       client_id=keycloak_client_id,
                                                                       client_secret=keycloak_client_secret,
                                                                       password=keycloak_password,
                                                                       workspace_id=workspace_id,
                                                                       host=keycloak_host,
                                                                       verify_ssl=False)

jwt = authentication_details.access_token


dataframe = kdp_conn.read_dataset_to_pandas_dataframe(dataset_id=dataset_id,
                                                      jwt=jwt,
                                                      starting_record_id=starting_record_id,
                                                      batch_size=batch_size)

end = timer()

print('Created pandas dataframe with', dataframe.size, 'elements (number of columns times number of rows) in ',
      end - start, 'seconds')

if dataframe.size < 5000:
    pprint(dataframe)
