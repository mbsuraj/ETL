class XLSXLoader:

    def __init__(self, data_file, spark=None, loader_config={}):

        assert spark is not None, "No spark session created"

        self._spark = spark.builder \
            .master("local") \
            .appName("Word Count") \
            .config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2") \
            .getOrCreate()
        self._data_file = data_file
        self._loader_config = loader_config

    def load_data(self):
        """

        :return: load data into spark session
        """

        return self._spark.read.options(**self._loader_config).csv(self._data_file)