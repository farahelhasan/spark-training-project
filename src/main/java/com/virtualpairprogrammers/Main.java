package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.stream.Collectors;

public class Main {
    private SparkSession spark;
    
	
	public Main() {
		spark = SparkSession.builder()
	            .appName("ETL Project")
	            .master("local[*]")
	            .config("spark.sql.warehouse.dir", "file:///C:/hadoop/warehouse")
	            .getOrCreate();
	}

    // Step 1:
    public void createHiveTable() {
	    // Load the cars.csv
	    Dataset<Row> carsDf = spark.read()
	        .option("header", "true")
	        .csv("src/main/resources/cars.csv");
	
	    // valid column names
	    Dataset<Row> renamedDf = carsDf.toDF(
	    	    Arrays.stream(carsDf.columns())
	    	          .map(column -> column.replaceAll("[^a-zA-Z0-9_]", "_")) 
	    	          .toArray(String[]::new)
	    );
	    
        // Data cleaning steps
	    renamedDf = renamedDf
                .filter(row -> !row.anyNull()) // Remove rows with null values
                .dropDuplicates(); // Remove duplicate rows

	
	    // Save the dataset as a Hive table
	    renamedDf.write().mode(SaveMode.Overwrite).saveAsTable("cars_table");
    }

    // Step 2: 
    public void extractCarModelAndOrigin() {
        
        Dataset<Row> theftDf = spark.read()
                .option("header", "true")
                .csv("src/main/resources/2015_State_Top10Report_wTotalThefts.csv");

        // vaild name for hive 
        theftDf = theftDf.toDF(
	    	    Arrays.stream(theftDf.columns())
	    	          .map(column -> column.replaceAll("[^a-zA-Z0-9_]", "_")) 
	    	          .toArray(String[]::new)
	    );
        // Data cleaning steps
        theftDf = theftDf
                .filter(row -> !row.anyNull()) // Remove rows with null values
                .dropDuplicates(); // Remove duplicate rows

        Dataset<Row> carsDf = spark.sql("SELECT * FROM cars_table");
        // cache the car table because i will use it in join 
        carsDf.cache(); 
        // join 
        Dataset<Row> allJoinData = theftDf.join(carsDf, theftDf.col("Make_Model").contains(carsDf.col("Car_Brand")), "inner") 
                .select(theftDf.col("*"), carsDf.col("Country_of_Origin"));

        // take special columns 
        Dataset<Row> extractedDf = allJoinData
                .select(theftDf.col("Make_Model"), carsDf.col("Country_of_Origin"));
        extractedDf = extractedDf.dropDuplicates();
        
        
        
        // Clean the Thefts column in-place
        allJoinData = allJoinData.withColumn("Thefts",
                functions.when(allJoinData.col("Thefts").isNotNull(),
                    functions.trim(functions.regexp_replace(allJoinData.col("Thefts"), ",", ""))
                ).otherwise(null)
        );

        // save all join data in hive 
        allJoinData.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable("thefts_table");
    

        // save as CSV file (Make/Model & Country_of_Origin)
        extractedDf.repartition(1).write()
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("output/extracted_car_model_and_origin");
        
      
    }
    
    // Step 3:
    public void partitionTheftData() {
        Dataset<Row> theftDf = spark.sql("SELECT * FROM thefts_table");
        // cache the thefts table 
        theftDf.cache();

        theftDf.write()
                .mode(SaveMode.Overwrite)
                .partitionBy( "Country_of_Origin")
                .saveAsTable("partitioned_thefts");
    }

    // Step 4:
    public void mergeUpdatedRecords() {
        try {
            // Load the original theft dataset from Hive
            Dataset<Row> originalDf = spark.sql("SELECT * FROM partitioned_thefts");
            // Cache the partitioned_thefts table because it will be used in join and select 
            originalDf.cache();
            
            // Load the updated dataset from the external CSV file
            Dataset<Row> updated = spark.read()
                    .option("header", "true")
                    .csv("src/main/resources/Updated - Sheet1.csv");   
                              
            Dataset<Row> carsDf = spark.sql("SELECT * FROM cars_table");
            // cache the car table because i will use it in join 
            carsDf.cache(); 
          
            // Data cleaning steps
            updated = updated
                    .filter(row -> !row.anyNull()) // Remove rows with null values
                    .dropDuplicates(); // Remove duplicate rows
            
            // Rename the columns to match the original ones
            Dataset<Row> updatedDf = updated.toDF(
                Arrays.stream(updated.columns())
                      .map(column -> column.replaceAll("[^a-zA-Z0-9_]", "_")) 
                      .toArray(String[]::new)
            );
            // join with car to add country of origin 
            updatedDf = updatedDf.join(carsDf, updatedDf.col("Make_Model").contains(carsDf.col("Car_Brand")), "inner") 
                    .select(updatedDf.col("*"), carsDf.col("Country_of_Origin"));
            
                    
            // Perform a full outer join on all columns from both datasets
            Dataset<Row> mergedDf = originalDf.join(updatedDf,
                    originalDf.col("Make_Model").equalTo(updatedDf.col("Make_Model"))
                    .and(originalDf.col("Model_Year").equalTo(updatedDf.col("Model_Year")))
                    .and(originalDf.col("State").equalTo(updatedDf.col("State"))),
                    "full_outer");

            Dataset<Row> finalDf = mergedDf.select(
                    functions.coalesce(updatedDf.col("Rank"), originalDf.col("Rank")).as("Rank"),
                    functions.coalesce(updatedDf.col("Make_Model"), originalDf.col("Make_Model")).as("Make_Model"),
                    functions.coalesce(updatedDf.col("Model_Year"), originalDf.col("Model_Year")).as("Model_Year"),
                    functions.coalesce(updatedDf.col("Thefts"), originalDf.col("Thefts")).as("Thefts"),
                    functions.coalesce(updatedDf.col("State"), originalDf.col("State")).as("State"),
                    functions.coalesce(updatedDf.col("Country_of_Origin"), originalDf.col("Country_of_Origin"))
            );

            // Clean the Thefts column in-place
            finalDf = finalDf.withColumn("Thefts",
                    functions.when(finalDf.col("Thefts").isNotNull(),
                        functions.trim(functions.regexp_replace(finalDf.col("Thefts"), ",", ""))
                    ).otherwise(null)
            );

            // Write the cleaned and merged dataset to a temporary location
            String tempTableName = "temp_partitioned_thefts";
            finalDf.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("Country_of_Origin")
                    .saveAsTable(tempTableName);

            finalDf.repartition(1).write()
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .csv("output/final");
            
            // Drop the original table
            spark.sql("DROP TABLE IF EXISTS partitioned_thefts");

            // Rename the temporary table to the original table name
            spark.sql("ALTER TABLE " + tempTableName + " RENAME TO partitioned_thefts");

            System.out.println("Merged dataset saved successfully.");
        } catch (Exception e) {
            System.err.println("Error during merging and saving: " + e.getMessage());
        }
  }

    // Step 5:
    public void extractTopCountries() {

        Dataset<Row> topCountriesDf = spark.sql(
            "SELECT Country_of_Origin, SUM(Thefts) AS Total_Thefts " +
            "FROM partitioned_thefts " +
            "GROUP BY Country_of_Origin " +
            "ORDER BY Total_Thefts DESC " +
            "LIMIT 5"
        );

        // Check for invalid or non-numeric data in Thefts column
        Dataset<Row> invalidData = spark.sql(
            "SELECT * FROM partitioned_thefts WHERE CAST(Thefts AS INT) IS NULL"
        );

        invalidData.repartition(1).write()
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("output/invalid_data");

        // Display the aggregated result
        topCountriesDf.show();

        // Save the cleaned result to a CSV file
        topCountriesDf.repartition(1).write()
            .option("header", "true")
            .mode(SaveMode.Overwrite)
            .csv("output/top_countries");

        System.out.println("Top countries extracted and saved successfully.");
        
        
//        
//        // Load the partitioned_thefts DataFrame (if not already loaded)
//        Dataset<Row> partitionedTheftsDf = spark.table("partitioned_thefts");
//
//        // Clean the data: remove commas and trim spaces, and filter valid numeric rows
//        Dataset<Row> cleanedDf = partitionedTheftsDf
//            .filter(partitionedTheftsDf.col("Thefts").isNotNull())  // Exclude NULL values
//            .withColumn("Cleaned_Thefts", functions.trim(functions.regexp_replace(partitionedTheftsDf.col("Thefts"), ",", ""))) // Remove commas
//            .filter(functions.col("Cleaned_Thefts").rlike("^[0-9]+$")); // Ensure valid numeric strings
//
//        // Aggregate the cleaned data
//        Dataset<Row> topCountriesDf = cleanedDf
//            .select(functions.sum(functions.col("Cleaned_Thefts").cast("int")).alias("Total_Thefts"));
//
//        // Display the aggregated result
//        topCountriesDf.show();
//
//        // Save the cleaned result to a CSV file
//        topCountriesDf.repartition(1).write()
//            .option("header", "true")
//            .mode(SaveMode.Overwrite)
//            .csv("output/top_countries");
//
//        System.out.println("Top countries extracted and saved successfully.");
    }


	public static void main(String[] args) {
		// Set Hadoop home directory
        System.setProperty("hadoop.home.dir", "C:/hadoop");

        // Configure logging
        Logger.getLogger("org.apache").setLevel(Level.WARN);
       
        Main etl = new Main();
        etl.createHiveTable(); // Step 1
        etl.extractCarModelAndOrigin(); // Step 2
        etl.partitionTheftData(); // Step 3
        etl.mergeUpdatedRecords(); // Step 4
        etl.extractTopCountries(); // Step 5
        
           
	}

}
