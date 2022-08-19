# kdp-examples

### Python Connector Examples

## Prequisites
* Python version 3.8.5
* Install dependencies from python-connector directory with:
```
cd python-connector
pip3 install -r requirements.txt
```

**Step 1**

Set System Variables to provide the values for the variables listed below:

Required
* EMAIL - KDP user's email address
* PASSWORD - KDP user's password
* WORKSPACE_ID - KDP user's workspace id
* DATASET_ID - KDP user's dataset id

Optional
* KDP_URL - KDP url to connect to, default is https://api.app.koverse.com
* PATH_TO_CSV_FILE - location to the csv file to be ingested, default=['https://kdp4.s3-us-east-2.amazonaws.com/test-data/cars.csv']
* STARTING_RECORD_ID - Record to start reading, default=''
* PATH_TO_CA_FILE - When not provided will not verify ssl of request, default=''
* INPUT_FILE - File with data to ingest, default='../datafiles/actorfilms.csv'
* BATCH_SIZE - number of records in a batch, default=100000

**Step 2**

Run the examples 

```
python3 <example_filename>
```
ex. python3 ingest_csv.py