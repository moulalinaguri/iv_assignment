from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

df = spark.read.option("multiline", "true").json("./dataset/input_data.json")

df.show()

df.printSchema()


def flatten_json(df):
    """
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

# Call the function to flatten the JSON DataFrame
df = flatten_json(df)
df.show()
df.printSchema()
