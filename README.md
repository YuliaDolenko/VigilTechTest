## Goal

A directory in S3 contains files with two columns

1. The files contain headers, the column names are just random strings and they are not consistent across files
2. Both columns are integer values
3. Some files are CSV some are TSV - so they are mixed, and you must handle this!
4. The empty string should represent 0
5. Henceforth the first column will be referred to as the key, and the second column the value
6. For any given key across the entire dataset (so all the files), there exists exactly one value that occurs an odd
   number of times.

Write an app in Scala that takes 3 parameters:

1. An S3 path (input)
2. An S3 path (output)
3. An AWS ~/.aws/credentials profile to initialise creds with, or param to indicate that creds should use default
   provider chain. Your app will assume these creds have full access to the S3 bucket.

Then in spark local mode the app should write file(s) to the output S3 path in TSV format with two columns such that The
first column contains each key exactly once The second column contains the integer occurring an odd number of times for
the key, as described by 6 above.

## Expected result
For input:
>col1,col2 \
>1,null \
>2,3 \
>2,3 \
>3,4 \
>3,4 \
>3,4

Output:
> key,   value,   odd_count \
> 1,  0,  1 \
> 3,  4,  3

## Dependencies

- Apache Spark 3.3.2 or later
- Scala 2.13.8 or later

## Usage

#### 1. Provide 3 program arguments:
- `args(0)` - Refers to the input path where the data to be processed is located. The input path should conform to the specified format below:


> Sample: `s3a://vigiltechtest/input/`

Here, `vigiltechtest` denotes the name of the S3 bucket and `input` is the directory containing the data.


- `args(1)` - Refers to the output path where the processed data will be stored. The output path should conform to the specified format below:

> Sample: `s3a://vigiltechtest/output/data`

Here, `vigiltechtest` denotes the name of the S3 bucket and `output/data` is the directories where the processed data will be saved.


- `args(2)` - Refers to the AWS access key and secret key, which are used for authentication. You can use the string "default" to read your keys from the `~/.aws/credentials` file.

> Sample: `"default"`

To use the `default` option, your `~/.aws/credentials` file should have the following format:
>[default] \
>aws_access_key_id = {ACCESS_KEY_ID} \
>aws_secret_access_key = {SECRET_ACCESS_KEY} 

Where `{ACCESS_KEY_ID}` and `{SECRET_ACCESS_KEY}` your actual values

#### 2. Validate the presence of the test data in the designated S3 bucket
It is crucial to ensure that the test data is available in the specified S3 bucket before running the processing methods. 
Additionally, it is essential to verify that the test data has the correct delimiter assigned to it: "," for CSV files 
and "\t" for TSV files.

#### 3. To establish and fine-tune the configuration of a distributed Spark application, there is an instantiated SparkSession object.
This object represents the entry point to the Spark application and allows us to specify various properties and settings, such as the application name, the master URL, and the cluster resources allocation.

It was done by code:
>val spark = SparkSession.builder \
>.appName("FileProcessing") \
>.config("spark.hadoop.fs.s3a.access.key", accessKey) \
>.config("spark.hadoop.fs.s3a.secret.key", secretKey) \
>.master("local[*]") \
>.getOrCreate()

In order to run the application locally it is using the "local[*]" master URL, which tells Spark to use all available CPU cores on the local machine.

#### 4. There are two different methods to calculate data
`processUsingAggregatesOnDf()` and `processUsingPlainSQL()`

The first implementation `processUsingAggregatesOnDf` uses DataFrames API to process the data. 
It first fills in missing values with 0, replaces empty string in the "value" column with 0, groups by "key" and "value", 
aggregates the count of rows with the same "key" and "value", filters out the rows with an even count, groups by "key" again, 
and finally selects the necessary columns and orders by "key".

The second implementation `processUsingPlainSQL` uses Spark SQL to process the data. 
It creates a temporary view for the input data, replaces empty string in the "value" column with 0, 
groups by "key" and "value", aggregates the count of rows with the same "key" and "value", filters out the rows with an even count, 
groups by "key" again, and finally selects the necessary columns and orders by "key". 
This implementation is more concise, but it uses SQL syntax which can be less familiar to some developers.
#### 5. The output file will be written to the specified output directory in TSV format
TSV (Tab-separated values) is a file format used to store and exchange data between different applications. 
It is similar to the CSV (Comma-separated values) format, but instead of using commas as a delimiter, 
it uses tabs to separate values.

## Complexity
`Receiver.readData`: 
- Time complexity: O(n), where n is the total number of rows in the input data.
- Space complexity: O(n), where n is the total number of rows in the input data.

`FileProcessing.processUsingAggregatesOnDf`:
- Time complexity: O(n log n), where n is the total number of rows in the input DataFrame. The groupBy and agg operations take O(n log n) time complexity.
- Space complexity: O(n), where n is the total number of rows in the input DataFrame.

`FileProcessing.processUsingPlainSQL`:
- Time complexity: O(n log n), where n is the total number of rows in the input DataFrame. The SQL query contains groupBy and having clauses, which take O(n log n) time complexity.
- Space complexity: O(n), where n is the total number of rows in the input DataFrame.

`Writer.writeOutput`:
- Time complexity: O(n), where n is the total number of rows in the input DataFrame.
- Space complexity: O(1). This method writes output to disk and does not use any extra space.


