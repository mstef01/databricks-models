from pyspark import pipelines as dp
import pandas as pd
import sys
sys.path.append("/Workspace/Repos/mstefan@hotmail.de/databricks-utils")
from utils import format_time_gap

# define the materialized view
@dp.materialized_view(
    name = "dhi_data.02_dhi_data_silver.wc_season_2026_silver",
    comment = "Silver table for cleaned DHI World Cup data.",
    table_properties = {"quality": "silver"}
)

def clean_data():
    spark_df = spark.read.table("dhi_data.01_dhi_data_bronze.wc_season_2026_bronze")
    df = spark_df.toPandas()

    # handle empty dataset (e.g. during dry run or full refresh before bronze is populated)
    if df.empty:
        return spark_df.drop(
            "ContextName", "Confirmed", "UciCode", "GivenName", "ScoreboardName",
            "UciTeamCode", "UciPoints", "SecondaryRank", "GroupIdx", "Protected", "IntEliteNr",
            "Rso", "Injury", "Substitute", "RotationOrder", "timing_id", "Id", "FamilyName"
        ).limit(0)

    # start data cleansing
    df_clean = df.drop(columns = ["ContextName","Confirmed", "UciCode", "GivenName", "GivenName", "ScoreboardName",
                        "UciTeamCode", "UciPoints", "SecondaryRank", "GroupIdx", "Protected", "IntEliteNr",
                        "Rso", "Injury", "Substitute", "RotationOrder", "timing_id", "Id", "FamilyName"
                        ]
             )

    # infer Privateer 
    df_clean["UciTeamId"] = df["UciTeamId"].map(lambda x: "PRIV" if x == "nan" else x)
    df_clean["UciTeamName"] = df["UciTeamName"].map(lambda x: "Privateer" if x == "nan" else x)
    # convert data types (using Int64 to handle NaN values)
    df_clean["Sector5_Position"] = df["Sector5_Position"].astype("Int64")
    df_clean["Sector4_Position"] = df["Sector4_Position"].astype("Int64")
    df_clean["Sector3_Position"] = df["Sector3_Position"].astype("Int64")
    df_clean["Sector2_Position"] = df["Sector2_Position"].astype("Int64")
    df_clean["Sector1_Position"] = df["Sector1_Position"].astype("Int64")
    # format dates
    df_clean["BirthDate"] = pd.to_datetime(df["BirthDate"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_clean["StartTime"] = pd.to_datetime(df["StartTime"], errors="coerce", unit="ms").dt.strftime("%H:%M:%S")
    # format sector times
    df_clean["Sector1_RaceTime"] = pd.to_datetime(df["Sector1_RaceTime"], errors="coerce", unit="ms").dt.strftime("%M:%S.%f").map(lambda x: x[:-3] if isinstance(x, str) else x)
    df_clean["Sector2_RaceTime"] = pd.to_datetime(df["Sector2_RaceTime"], errors="coerce", unit="ms").dt.strftime("%M:%S.%f").map(lambda x: x[:-3] if isinstance(x, str) else x)
    df_clean["Sector3_RaceTime"] = pd.to_datetime(df["Sector3_RaceTime"], errors="coerce", unit="ms").dt.strftime("%M:%S.%f").map(lambda x: x[:-3] if isinstance(x, str) else x)
    df_clean["Sector4_RaceTime"] = pd.to_datetime(df["Sector4_RaceTime"], errors="coerce", unit="ms").dt.strftime("%M:%S.%f").map(lambda x: x[:-3] if isinstance(x, str) else x)
    df_clean["Sector5_RaceTime"] = pd.to_datetime(df["Sector5_RaceTime"], errors="coerce", unit="ms").dt.strftime("%M:%S.%f").map(lambda x: x[:-3] if isinstance(x, str) else x)
    # format sector gaps
    df_clean["Sector1_TimeGap"] = df["Sector1_TimeGap"].map(lambda x: format_time_gap(x))
    df_clean["Sector2_TimeGap"] = df["Sector2_TimeGap"].map(lambda x: format_time_gap(x))
    df_clean["Sector3_TimeGap"] = df["Sector3_TimeGap"].map(lambda x: format_time_gap(x))
    df_clean["Sector4_TimeGap"] = df["Sector4_TimeGap"].map(lambda x: format_time_gap(x))
    df_clean["Sector5_TimeGap"] = df["Sector5_TimeGap"].map(lambda x: format_time_gap(x))
    # format speed
    df_clean["Speed"] = df["Speed"].round(2)

    return spark.createDataFrame(df_clean)