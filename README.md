## Sample Configuration

```
"source": {
    "s_df_1": {
        "sourceType": "csv",
        "sourcePath": "C:/Platform_data_ingestion_tracks-main/Input_DL_RZ/testmetadata.csv",
        "sourceschema" : " \"A\" ,\"X\" ,\"B\" ,\"Y\" ,\"D\" ,\"E\",\"H\",\"G\" ,\"I\" "
    },
    "s_df_2": {
        "sourceType": "parquet",
        "sourcePath": "C:/Platform_data_ingestion_tracks-main/Input_DL_RZ/wo_parquet",
        "sourceschema" : ""
    },
    "s_df_3": {
        "sourceType": "Mysql",
        "sourceInfo": "",
        "sourceschema" : ""
    }
}
```