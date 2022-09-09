# Databricks notebook source
container = 'filipe-nikolofski'
storage_account_name = 'mafistorage'
storage_account_key = '8Euv9yGkNMXEJKEcwQBh3CpKa3F9D7pjFPYIxzheo00LGeW6d7bdKcl/0eA0tVPQNVIOihaEpvP3+AStuX3N4g=='
file_name = "agregados58c3bac1b4276af342f8cba8d71c333d2e8dbc1a171c37a9ef29cc20ee87f679"
PATH = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/{file_name}"

# COMMAND ----------

spark.conf.set(f"spark.hadoop.fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key) 

# COMMAND ----------

schema = StructType([
      StructField("Id",IntegerType(),True),
      StructField("nome",StringType(),True)
  ])

# COMMAND ----------



from pyspark.sql.functions import *
from pyspark.sql.types import *


json_literacy1 = (spark
                  .read
                  .json(PATH)
                  .withColumn("agregados", explode(col("agregados")))          
                 )

# COMMAND ----------


display(json_literacy1)

# COMMAND ----------


