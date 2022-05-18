**Project Description**
Sparkify wants to move their data from the data warehouse to a data lake.
As a data engineer, I am helping Sparkify in building ETL pipelines from data warehouse to data lake.
The data is extracted from S3, processed using Spark and loaded to Data back to S3 as a set of dimensional tables.

**Scripts explanation**
etl.py :
i) read song_data and load_data from S3,
ii) transform data to create five different tables,
iii) and write tables to partitioned parquet files in table directories on S3.


**How to run the Python scripts**
1. In the dl.cfg, enter the Access Key ID and Secret Access Key
2. Run the etl.py script