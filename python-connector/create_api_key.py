import os
from kdp_connector import KdpConn
import kdp_api

# This example shows you how to use the KDP Python Connector to create and use an api key to access the KDP platform

# Required environment variables: email, password, workspace_id
# ########## variables ###########################################
# authentication code
email = os.environ.get('EMAIL', default=None)
password = os.environ.get('PASSWORD', default=None)

# workspace id
workspace_id = os.environ.get('WORKSPACE_ID')

# host url to KDP platform
kdp_url = os.environ.get('KDP_URL', default='https://api.app.koverse.com')

# OPTIONAL: When not provided will not verify ssl of request. Which will result in warnings in the log.
# See configuration/configurationUtil.py for more detail about using certificates for ssl.
path_to_ca_file = os.environ.get('PATH_TO_CA_FILE', default='')

#################################################################

# Construct kdpConnector
kdp_conn = KdpConn(path_to_ca_file, kdp_url, discard_unknown_keys=True)

# Login as an admin workspace user
if (email is not None) and (password is not None):
    authentication_details = kdp_conn.create_and_set_authentication_token(email=email,
                                                                          password=password,
                                                                          workspace_id=workspace_id)

# Add attributes -- example 'attributeNames': ["eastern", "western"],
# and groups for dataset access -- example 'groupNames': ["write_group"]
# You can configure or change access after creation, from the koverse UI (if you have the necessary permissions)
api_key_create_request = kdp_api.models.ApiKeyCreateRequest.from_dict({
    "name": "Example API Key",
    "workspaceId": f"{workspace_id}",
    "description": f"Example API Key for {workspace_id} workspace",
    "groupNames": [],
    "attributeNames": []
})

config = kdp_conn.create_configuration()

with (kdp_api.ApiClient(config) as api_client):
    api = kdp_api.UsersAndGroupsApi(api_client)
    api_key = api.post_api_keys(api_key_create_request)

    print(f"created api_key: {api_key}")

    # Create a new connection with the api_key
    new_kdp_conn = KdpConn(path_to_ca_file, kdp_url, discard_unknown_keys=True, api_key=api_key.key)
    new_config = new_kdp_conn.create_configuration()

    with kdp_api.ApiClient(new_config) as new_api_client:
        # Example of using the api_key with the kdp-api-python-client to get groups from kdp
        users_and_groups_api = kdp_api.UsersAndGroupsApi(new_api_client)
        groups = users_and_groups_api.get_groups()
        print(f"groups: {groups}")

        # Example of using the api_key with the kdp-api-python-client to get workspaces from kdp
        workspace_api = kdp_api.WorkspacesApi(new_api_client)
        workspaces = workspace_api.get_workspaces()
        print(f"workspaces: {workspaces}")





