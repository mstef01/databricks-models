from pyspark import pipelines as dp
import pandas as pd

@dp.materialized_view(
  name = "dhi_data.01_dhi_data_bronze.wc_season_2026_bronze",
  comment = "This table contains all the bronze layer raw timing data for the DHI World Cup season.",
  table_properties = {"quality": "bronze"}
  )
def season_data():
  # Read data using pandas (index_col=0 treats first column as index, not a data column)
  df = pd.read_csv("/Volumes/dhi_data/00_dhi_data_landing/vol_s3_dhi_data/seasons/race_data_2026.csv", index_col=0)
  
  # Convert pandas DataFrame to Spark DataFrame and return
  return spark.createDataFrame(df)