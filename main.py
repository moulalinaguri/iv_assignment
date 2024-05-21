from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.option("multiline", "true").json("./dataset/input_data.json")

    return df


def transform_data(df):
    """Transform original dataset.

    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    Flattens a DataFrame with complex nested fields (Arrays and Structs) by converting them into individual columns.

    Parameters:
    - df: The input DataFrame with complex nested fields

    Returns:
    - The flattened DataFrame with all complex fields expanded into separate columns.
    """
    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    print(df.schema)
    print("")
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df


def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    # Write DataFrame to Amazon OpenSearch
    df.write.format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "<OpenSearch_Host>") \
        .option("es.port", "<OpenSearch_Port>") \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", "<OpenSearch_Index>/<OpenSearch_Type>") \
        .save()

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
