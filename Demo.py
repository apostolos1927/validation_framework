# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM test_data order by id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transformed_data order by id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_data2 order by id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transformed_data2 order by id

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from enum import Enum
from pyspark.sql.functions import *



primary_keys = ['id']
columns_to_exclude = ['email']

class Columns(Enum):
    test_case_id = 'test_case_id'
    dataset_name = 'dataset_name'
    key_columns = 'key_columns'
    key_columns_values = 'key_columns_values'
    column_name = 'column_name'
    column_value = 'column_value'
    column_value_test = 'column_value_test'
    column_value_tranformed = 'column_value_tranformed'
    is_valid = 'is_valid'


def combine_unpivoted_dfs(df_test_data_unpivoted, df_transformed_data_unpivoted,transformed_table_name):

    keys_with_column_name = primary_keys.copy()
    keys_with_column_name.append(Columns.column_name.value)
    print('keys_with_column_name :',keys_with_column_name)
   
    combined_df = (df_test_data_unpivoted.alias("df_test_data_unpivoted").filter(col(f"df_test_data_unpivoted.{Columns.column_name.value}") != Columns.test_case_id.value).join(df_transformed_data_unpivoted.alias("df_transformed_data_unpivoted"),keys_with_column_name,"full")
        .withColumn(Columns.dataset_name.value, lit(transformed_table_name.split('_')[1]))
        .withColumn(Columns.key_columns.value, lit('|'.join(primary_keys)))
        .withColumn(Columns.key_columns_values.value, concat_ws('|', *primary_keys))
        .withColumn(Columns.column_value_test.value, col(f"df_test_data_unpivoted.{Columns.column_value.value}"))
        .withColumn(Columns.column_value_tranformed.value, col(f"df_transformed_data_unpivoted.{Columns.column_value.value}"))
        .withColumn(Columns.is_valid.value,
                    when(col(Columns.column_value_test.value) == col(Columns.column_value_tranformed.value), True)
                    .when(col(Columns.column_value_test.value).isNull() & col(Columns.column_value_tranformed.value).isNull(), True)
                    .otherwise(False))
        .drop(Columns.column_value.value)
        .drop(*primary_keys)
        )
    print("combined dataset is :")
    combined_df.display()

    df_with_test_case = (df_test_data_unpivoted.filter(col(Columns.column_name.value) == Columns.test_case_id.value)
        .withColumn(Columns.key_columns_values.value, concat_ws('|', *primary_keys))
        .withColumnRenamed(Columns.column_value.value, Columns.test_case_id.value)
        .drop(Columns.column_name.value)
    )
    
    print("testcase dataset is :")
    df_with_test_case.display()
 
    result_df = combined_df.join(df_with_test_case, Columns.key_columns_values.value, 'inner')
    print("final dataset is :")
    result_df.select([column.value  for column in Columns if column.value != Columns.column_value.value]).display()
    return result_df.select([column.value  for column in Columns if column.value != Columns.column_value.value])


def melt_df(df, primary_keys, exclude_cols):
    cols_to_melt = [col for col in df.columns if col not in primary_keys and col not in exclude_cols]
    print(cols_to_melt)
    df_unpivot = df.select(primary_keys + [col(c).cast("string").alias(c) for c in cols_to_melt])\
        .melt(
            ids=primary_keys, values=cols_to_melt, variableColumnName=Columns.column_name.value,
            valueColumnName=Columns.column_value.value
        )
    return df_unpivot

def execute(test_table_name, transformed_table_name):
        df_test_data = spark.read.table(test_table_name).fillna('')
        df_transformed_data = spark.read.table(transformed_table_name).fillna('')
        df_test_data_unpivoted = melt_df(df_test_data, primary_keys,columns_to_exclude)
        print('df_test_data_unpivoted')
        df_test_data_unpivoted.display()
        df_transformed_data_unpivoted = melt_df(df_transformed_data, primary_keys, columns_to_exclude)
        print('df_transformed_data_unpivoted')
        df_transformed_data_unpivoted.display()
        result_df = combine_unpivoted_dfs(df_test_data_unpivoted, df_transformed_data_unpivoted,transformed_table_name)
        result_df.write.mode('overwrite').saveAsTable(f"results_{transformed_table_name}")


if __name__=='__main__':
    tables = {'test_data':'transformed_data', 
              'test_data2':'transformed_data2'
              }
    for test_table_name, transformed_table_name in tables.items():
        execute(test_table_name, transformed_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM results_transformed_data order by key_columns_values
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM results_transformed_data2 order by key_columns_values
