## Spark Connector examples

Documentation for the KDP Spark Connector can be found at [Koverse.com](https://documentation.koverse.com/)

### Running the examples

To run an example, go through and update the workspaceId, datasetId, etc for your KDP workspace. Run 'mvn package' to build the example. 

You will need to set the environmental variables for your username(email) and password for authentication to KDP.

```
export KDP_EMAIL=myemail@mycompanydomain.com
export KDP_PSWD=AbadP@ssw0rd
```

To run you will need to install Spark (or run the application from your Spark cluster). See [Spark latest documention](https://spark.apache.org/docs/latest/) for installation and instructions on running via spark-submit locally or on a cluster.

Example command:
```
$SPARK_HOME/bin/spark-submit ./target/kdp-spark-example.jar --master local[4]
```

### KDP Count

This example, in Java, covers the main functionality of the KDP Spark Connector. It includes read, write, and simple ABAC. You will need Spark 3, Maven 3, and Java 8 11 to build and run this example.

You will first need to add a dataset to your workspace. Go into the src/main/resources directory and upload the file kdp-count-records.json. This will be the datasetId you will start with. You will need to update the variables for workspaceId, datasetId, datasetName to run this example.

After you've run the example you will see 2 new datasets in your workspace and some new user attributes. You should see all of the data in the "kdp-spark-test" dataset. You won't see records in the "kdp-spark-test-abac" dataset. Play with assigning the attributes and then look back at the "-abac" dataset.

Please see [KdpCount.java](kdp-spark-count-example/src/main/java/com/koverse/spark/KdpCount.java) for a walk through of the code.