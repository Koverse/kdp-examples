package com.koverse.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KdpCount {

  public static void main(String[] args){

    // You will first need to add a dataset to your workspace. Go into the src/main/resources directory and upload the file kdp-count-records.json. This will be the datasetId you will start with below. Do not set up ABAC for this dataset.


    // This is the default. KdpUrl is not required but it's included here as an example.
    String kdpUrl = "https://api.app.koverse.com";

    SparkSession spark = SparkSession
      .builder()
      .appName("KDPCount")
      .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");

    // workspaceId is required for both read and write
    String workspaceId = "";

    // datasetId is the UUID of the dataset you wish to read from. You can find it in the address bar in the KDP UI. Just select the dataset you wish to read from.
    String datasetId = "";

    // Now let's read in the data from our dataset into a Spark Dataframe
    Dataset<Row> randomDF = spark.read().format("com.koverse.spark.read")
        .option("workspaceId", workspaceId)
        .option("datasetId",datasetId)
        .option("kdpUrl",kdpUrl)
        .load();

    randomDF.show();

    String columnName = "pet";
    Dataset<Row> counts = randomDF.groupBy(columnName).count().as("count");

    // When writing to KDP the Connector will first create a new dataset.
    // Appending to any existing dataset is not available at this time.
    // You will need to pass in a name for your dataset.
    // Dataset names are not unique but the datasetId that will be assigned to it is.

    String datasetName = "kdp-spark-test";

    // Writing your Dataframe to KDP requires the datasetName, workspaceId
    counts.write().format("com.koverse.spark.write")
        .option("workspaceId", workspaceId)
        .option("datasetName", datasetName)
        .option("kdpUrl",kdpUrl)
        .mode("Append").save(); // here Append is used because the process of saving is create dataset, then save to that dataset.

    // Writing to a dataset with ABAC configured is simple:
    // You will need to pick a parser: https://documentation.koverse.com/docs/datasets/abac-attributes/abac-attribute-fields/
    // Single Attribute ABAC Label - simple-parser
    // Attribute Expression ABAC Label - identity-parser
    String parser = "simple-parser";

    // Labeled Fields is a comma separated list of fields where the attributes or expressions can be found.
    String labeledFields = "label";

    // You will need to configure the handling policy. This instructs KDP on how to handle records where the label fields are not present or are empty.
    String handlingPolicy = "drop";  // Include the record and leave the labeled field empty or missing.
    String replacementString = ""; // This is the string to used if handling policy "replace" is choosen. If "replace" is chosen this field is required otherwise it is ignored.

    randomDF.write().format("com.koverse.spark.write")
        .option("labelParser",parser)
        .option("labeledFields",labeledFields)
        .option("handlingPolicy",handlingPolicy)
        .option("replacementString",replacementString)
        .option("workspaceId", workspaceId)
        .option("datasetName", datasetName + "-abac")
        .option("kdpUrl",kdpUrl)
        .mode("Append").save();

  }
}
