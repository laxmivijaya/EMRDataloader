from pyspark.sql import SparkSession
from pyspark.sql.functions import col


S3_DATA_SOURCE_PATH = 's3://jinmeister-bucket-123456/data-source/survey_results_public.csv'
S3_DATA_OUTPUT_PATH = 's3://jinmeister-bucket-123456/data-output'

def main():
    spark = SparkSession.builder.appName('JinmeisterDemoApp').getOrCreate()
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)
    print('Total number of records in the source data: %s' % all_data.count())
    selected_data = all_data.where((col('Country') == 'United States of America') & (col('ResponseId') < 10001))
    print('The number of engineers who are in the first 10000 in the US is: %s' % selected_data.count())
    selected_data.write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH)

    selected_data.write.format('csv').mode('overwrite').save(S3_DATA_OUTPUT_PATH)
    print('Selected data was successfully saved to s3: %s' % S3_DATA_OUTPUT_PATH)

if __name__ == '__main__':
   main()
