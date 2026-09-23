import pandas as pd
from pyspark import pipelines as dp
import sys
sys.path.append("/Workspace/Repos/mstefan@hotmail.de/databricks-utils")
from utils import format_sector

@dp.materialized_view(
    name = "dhi_data.03_dhi_data_gold.wc_season_2026_gold",
    comment = "Gold table for DHI World Cup data.",
    table_properties = {"quality": "silver"}
)

def gold_data():
    spark_df_silver = spark.read.table("dhi_data.02_dhi_data_silver.wc_season_2026_silver")
    spark_df_bronze = spark.read.table("dhi_data.01_dhi_data_bronze.wc_season_2026_bronze")
    df_silver = spark_df_silver.toPandas()
    df_bronze = spark_df_bronze.toPandas()
    df_gold = df_silver.drop(columns = [])

    if df_silver.empty:
        return spark_df_silver.limit(0)
    
    # calc sector times
    df_gold["Sector1"] = df_silver["Sector1_RaceTime"]
    df_gold["Sector2"] = (df_bronze["Sector2_RaceTime"] - df_bronze["Sector1_RaceTime"]).map(lambda x: format_sector(x))
    df_gold["Sector3"] = (df_bronze["Sector3_RaceTime"] - df_bronze["Sector2_RaceTime"]).map(lambda x: format_sector(x))
    df_gold["Sector4"] = (df_bronze["Sector4_RaceTime"] - df_bronze["Sector3_RaceTime"]).map(lambda x: format_sector(x))
    df_gold["Sector5"] = (df_bronze["Sector5_RaceTime"] - df_bronze["Sector4_RaceTime"]).map(lambda x: format_sector(x))

    return spark.createDataFrame(df_gold)