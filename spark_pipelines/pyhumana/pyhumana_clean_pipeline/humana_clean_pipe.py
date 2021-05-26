from src.load import loader_adaptor_factory as laf
from spark_pipelines.pyhumana.pyhumana_clean_pipeline import null_cleaner, organizer
from data_catalog.catalog import catalog
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pprint import pprint
import pandas as pd


def main():
    spark = SparkSession
    data_file = catalog['landing/composite']
    options = {'header': 'true',
               'mode': 'DROPMALFORMED'}

    # Load data
    data_loader_adaptor = laf.LoaderAdaptorFactory.GetLoaderAdaptor(data_file=data_file)
    data_loader = data_loader_adaptor(data_file=data_file, spark=spark, options=options)
    humana_df = data_loader.load_data()
    pprint(humana_df.show())

    # organize the data based on the rules stated
    # based on rules stated in catalog['landing/event_attributes']
    df_rules = pd.read_csv(catalog['landing/event_attributes'], index_col=[0])
    df_rules.to_dict('index')
    org = organizer.Organizer(df=humana_df, df_rules=df_rules, save=False)
    humana_df = org.organize()
    pprint(humana_df.show())

    # clean null data
    # define schema
    integer_columns = []
    short_columns = ['Days']
    string_columns = [c for c in humana_df.columns if c not in integer_columns + short_columns]
    cast_dict = {
        IntegerType: integer_columns,
        ShortType: short_columns,
        StringType: string_columns
    }
    for cast, cast_list in cast_dict.items():
        for column in cast_list:
            humana_df = humana_df.withColumn(column, col(column).cast(cast()))
    n_cln = null_cleaner.NullCleaner(spark_df=humana_df, save=True)
    n_cln.clean(save_location=catalog['clean/composite'])


if __name__ == "__main__":
    main()
