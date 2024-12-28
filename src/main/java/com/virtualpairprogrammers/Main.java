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

        
        // save all join data in hive 
        allJoinData.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable("thefts_table");
    

        // save as CSV file (Make/Model & Country_of_Origin)
        extractedDf.write()
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
                .partitionBy( "Country_of_Origin", "State")
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
                    originalDf.col("Country_of_Origin")
            );

            // Write the merged dataset to a temporary location
            String tempTableName = "temp_partitioned_thefts";
            finalDf.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("Country_of_Origin", "State")
                    .saveAsTable(tempTableName);

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
     // Clean and aggregate data for the USA
        Dataset<Row> topCountriesDf = spark.sql(
                "SELECT State, SUM(Thefts) AS Total_Thefts " +
                "FROM partitioned_thefts " +
                "WHERE Country_of_Origin = 'America' " + 
                "GROUP BY State " +
                "ORDER BY Total_Thefts DESC " +
                "LIMIT 5"
        );
        
        // Data cleaning steps
        topCountriesDf = topCountriesDf
                .filter(row -> !row.anyNull()) // Remove rows with null values
                .dropDuplicates(); // Remove duplicate rows

        topCountriesDf.show();

        // Save the cleaned result to a CSV file
        topCountriesDf.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv("output/top_5_countries");

        System.out.println("Top 5 countries extracted and saved successfully.");
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
