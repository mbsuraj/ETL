from pyspark.sql.types import *
from pprint import pprint

class NullCleaner:

    def __init__(self, spark_df, schema=None, numeric_fill=0, decimal_fil=0.0, string_fill="", save=True):

        assert spark_df is not None, "spark_df must be given"
        self._spark_df = spark_df
        self._schema = schema if schema is not None else spark_df.schema
        self._numeric_fill = numeric_fill
        self._decimal_fill = decimal_fil
        self._string_fill = string_fill
        self._save = save
        self._numeric_columns = [col.name for col in self._schema if type(col.dataType) in [IntegerType, ShortType]]
        self._string_columns = [col.name for col in self._schema if type(col.dataType) is StringType]
        self._decimal_columns = [col.name for col in self._schema if type(col.dataType) in [DoubleType, DecimalType]]

    def clean(self, save_location=None):
        if self._save:
            assert save_location is not None, "Please set NullCleaner parameter 'save' to False if saving is not needed"
        else:
            assert save_location is None, "Please set NullCleaner parameter 'save' to True if saving is needed"
        clean_dict = {**self._get_numeric_dict(), **self._get_string_dict(), **self._get_decimal_dict()}
        self._spark_df = self._spark_df.na.fill(clean_dict)
        pprint(self._spark_df.show())
        if self._save:
            self._spark_df.write.parquet(save_location)
        else:
            return self._spark_df

    def _get_numeric_dict(self):
        fill_dict = {x: self._numeric_fill for x in self._numeric_columns}
        return fill_dict

    def _get_decimal_dict(self):
        fill_dict = {x: self._decimal_fill for x in self._decimal_columns}
        return fill_dict

    def _get_string_dict(self):
        fill_dict = {x: self._string_fill for x in self._string_columns}
        return fill_dict