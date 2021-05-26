from pyspark.sql import SparkSession

class CSVLoader:

    def __init__(self, data_file, spark=None, options={}, schema=None):

        assert spark is not None, "No spark session created"
        assert spark is SparkSession, f"Only SparkSession needed; instead {spark} given"
        self._data_file = data_file
        self._spark = self._setup_spark_session(spark)
        self._options = options
        self._schema = schema

    def _setup_spark_session(self, spark):

        return spark.builder.getOrCreate()

    def load_data(self):
        """

        :return: load data into spark session
        """
        if self._schema is not None:
            df = self._spark.read.options(**self._options).schema(self._schema).csv(self._data_file)
        else:
            df = self._spark.read.options(**self._options).csv(self._data_file)
        return df