from pyspark.sql.functions import  col, create_map, lit
import pyspark.sql.functions as sqlf
from itertools import chain

def map_to_new_column(df, category_mapping, from_col, to_col):
    mapping_expr = create_map([lit(x) for x in chain(*category_mapping.items())])
    df = df.withColumn(to_col, mapping_expr.getItem(col(from_col)))
    return df

def drop_null_columns(df):
    """
    This function drops columns containing all null values.
    :param df: A PySpark DataFrame
    """

    null_counts = df.select([sqlf.count(sqlf.when(sqlf.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[
        0].asDict()
    to_drop = [k for k, v in null_counts.items() if v >= df.count()]
    df = df.drop(*to_drop)

    return df