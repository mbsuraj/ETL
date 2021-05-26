from pyspark.sql import DataFrame
import pandas as pd
import numpy as np
from pyspark.sql.functions import lit
from src.cleaning.transforming import pandas_transformers_pool as ptp
from src.cleaning.transforming import spark_transformers_pool as stp


class Organizer:

    def __init__(self, df=None, df_rules=None, save=True):

        assert df is not DataFrame, f"df cannot be type(df).It must be {DataFrame}"
        assert df_rules is not pd.DataFrame, f"df_rules cannot be type(df). It must be {pd.DataFrame}"

        self._df_rules = df_rules
        self._df = df
        self._save = save

        self._preprocess_df_rules()
        self._map_event_category()
        self._reset_rules_to_event_category()

    def _preprocess_df_rules(self):
        self._df_rules = ptp.convert_to_lower_case(self._df_rules)
        rename_dict = {
            'Attribute1': 'event_attr1',
            'Attribute2': 'event_attr2',
            'Attribute3': 'event_attr3',
            'Attribute4': 'event_attr4',
            'Attribute5': 'event_attr5',
            'Attribute6': 'event_attr6',
            'Attribute7': 'event_attr7',
            'Attribute8': 'event_attr8',
            'Attribute9': 'event_attr9',
            'Attribute10': 'event_attr10',
        }

        self._df_rules = self._df_rules.rename(columns=rename_dict)
        self._df_rules = ptp.insert_underscore_for_space(self._df_rules)
        self._df_rules.rename(columns=rename_dict, inplace=True)

    def _map_event_category(self):
        category_mapping = self._df_rules.iloc[:, 0:1].to_dict()['Event Category']
        self._df = stp.map_to_new_column(df=self._df,
                                        category_mapping=category_mapping,
                                        from_col="event_descr",
                                        to_col='event_category')

    def _reset_rules_to_event_category(self):
        self._df_rules.drop_duplicates(inplace=True)
        self._df_rules.reset_index(inplace=True, drop=True)
        self._df_rules.set_index('Event Category', inplace=True)


    def organize(self, save_location=None):
        if self._save:
            assert save_location is not None, "Please set NullCleaner parameter 'save' to False if saving is not needed"
        else:
            assert save_location is None, "Please set NullCleaner parameter 'save' to True if saving is needed"
        all_non_event_attributes = [x for x in self._df.columns if x.find('attr') == -1]
        all_event_attributes = self._df_rules.values.flatten()
        all_event_distinct_attributes = [x for x in set(all_event_attributes) if x is not np.nan]
        event_category_attribute_relation = self._df_rules.to_dict('index')

        organized_df = None
        for event_category in event_category_attribute_relation.keys():
            temp = self._df.filter(self._df.event_category == event_category)
            for from_col, to_col in event_category_attribute_relation[event_category].items():
                if to_col is not np.nan:
                    temp = temp.withColumnRenamed(from_col, to_col)
                else:
                    temp = temp.drop(from_col)
            # get the attributes out of all event distinct attributes that
            # were not used as a new blank column
            new_untouched_columns = [*set(all_event_distinct_attributes) \
                .difference(set(event_category_attribute_relation[event_category].values()))]

            for new_col in new_untouched_columns:
                temp = temp.withColumn(new_col, lit(None))

            # arranging into a sequence of columns
            all_attributes = all_non_event_attributes[0:2] + \
                             all_non_event_attributes[-1:] + \
                             [cat_attr for cat_attr in event_category_attribute_relation[event_category].values() if
                              cat_attr is not np.nan] + \
                             new_untouched_columns + \
                             all_non_event_attributes[2:-1]
            if organized_df is None:
                organized_df = temp.select(*all_attributes)
            else:
                organized_df = organized_df.union(temp)
            organized_df = stp.drop_null_columns(organized_df)
            if self._save:
                organized_df.write.parquet(save_location)
            else:
                return organized_df