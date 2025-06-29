from pyspark.sql.functions import sha2, concat_ws, coalesce, col, lit, to_timestamp, md5
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable
import importlib, sys

from DatabaseConstants import TableNames, ProjectColumns, AssetGroupColumns, AssetColumns, FieldColumns, IncrementalLoadColumns, AssetHistoryColumns, AssetStatusColumns, HistoricalLoadColumns, HolidayCalendarColumns
import DIFFieldConversion

G_TempSuffix = "_temp"

G_LoadTypeInstructions = {
    "T1-All-Replace": {"Description": "Get all data and completely replace what is there. No C Table.", "HashColumns": False, "VersionColumn": False, "FromToColumns": False, "CTable": False, "Delete": "All", "LatestView": True, "Merge": False},
    "T1-Subset-Append": {"Description": "Append to T table. No C Table.", "HashColumns": False, "VersionColumn": False, "FromToColumns": False, "CTable": False, "Delete": "None", "LatestView": False, "Merge": False},
    "T1-Segment-Delete": {"Description": "Delete segment. No C Table.", "HashColumns": False, "VersionColumn": False, "FromToColumns": False, "CTable": False, "Delete": "Hard.Segment", "LatestView": False, "Merge": False},
    "T2-All-None-VersionID": {"Description": "Append with version ID. No C Table.", "HashColumns": False, "VersionColumn": True, "FromToColumns": False, "CTable": False, "Delete": "None", "LatestView": False, "Merge": False},
    "T2-All-None-FromTo": {"Description": "Full data; changes with From-To columns", "HashColumns": True, "VersionColumn": False, "FromToColumns": True, "CTable": True, "Delete": "None", "LatestView": True, "Merge": False},
    "T2-Subset-None-FromTo": {"Description": "Subset data; mark changes", "HashColumns": True, "VersionColumn": False, "FromToColumns": True, "CTable": True, "Delete": "None", "LatestView": False, "Merge": False},
    "T2-All-SenseDelete-FromTo": {"Description": "Full data; mark changes, sense delete", "HashColumns": True, "VersionColumn": False, "FromToColumns": True, "CTable": True, "Delete": "MissingKeys", "LatestView": True, "Merge": False},
    "T4-All-None": {"Description": "Full data; mark changes", "HashColumns": True, "VersionColumn": False, "FromToColumns": False, "CTable": True, "Delete": "None", "LatestView": False, "Merge": True},
    "T4-All-SenseDelete": {"Description": "Full data; mark changes, soft delete missing keys", "HashColumns": True, "VersionColumn": False, "FromToColumns": False, "CTable": True, "Delete": "MissingKeys", "LatestView": False, "Merge": True},
    "T4-Subset-SourceDeleteFlag": {"Description": "Subset; delete flagged records", "HashColumns": True, "VersionColumn": False, "FromToColumns": False, "CTable": True, "Delete": "DeleteFlag", "LatestView": False, "Merge": True},
    "T4-Subset-None": {"Description": "Subset of data; mark changes", "HashColumns": True, "VersionColumn": False, "FromToColumns": False, "CTable": True, "Delete": "None", "LatestView": False, "Merge": True},
    "T4-Segment-Delete": {"Description": "Subset data; delete segment", "HashColumns": True, "VersionColumn": False, "FromToColumns": False, "CTable": True, "Delete": "Soft.Segment", "LatestView": False, "Merge": True}
}

class DIFProcessedTableHelper:
    def __init__(self, pEnvConfig, pAssetGroupConfig, pLogger, pSpark, pAsset):
        self.aEnvConfig = pEnvConfig
        self.aAssetGroupConfig = pAssetGroupConfig
        self.aLogger = pLogger
        self.aSpark = pSpark
        self.aAsset = pAsset
        self.aInstructions = G_LoadTypeInstructions[pAsset[f'{AssetColumns.Load_Type_Code}']]

    def getNextVersionID(self):
        try:
            self.aLogger.debug("getNextVersionID.Start")
            df = self.aSpark.sql(
                "SELECT MAX(VERSION_ID) AS maxversion FROM " + self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            x = df.collect()[0][0]
            x = 0 if x is None else x + 1
            self.aLogger.debug("getNextVersionID.End")
            return x
        except Exception as ex:
            self.aLogger.error("getNextVersionID.Error: " + str(ex))
            raise Exception(ex)

    def getDeleteFieldAndValue(self):
        try:
            self.aLogger.debug("getDeleteFieldAndValue.Start")
            for dField in self.aAsset["Fields"]:
                if "Delete" in dField.get("ActionFlags", ""):
                    field_name = dField[f'{FieldColumns.Target_Field_Name}']
                    field_value = dField["ActionFlags"].split("=")[1]
                    self.aLogger.debug("getDeleteFieldAndValue.End")
                    return field_name, field_value
            self.aLogger.info("Could not find ActionFlags field with 'Delete'")
            return "", ""
        except Exception as ex:
            self.aLogger.error("getDeleteFieldAndValue.Error:" + str(ex))
            raise Exception(ex)

    def getAdjustedRawDataFrame(self, pDF):
        try:
            self.aLogger.debug("getAdjustedRawDataFrame.Start")
            df = pDF
            df = self.addVersionIDColumn(df)
            df = self.addHashColumns(df)
            df = self.addFromToColumns(df)
            self.aLogger.debug("getAdjustedRawDataFrame.End")
            return df
        except Exception as ex:
            self.aLogger.error("getAdjustedRawDataFrame.Error: " + str(ex))
            raise Exception(ex)

class DIFProcessedTableHelper:
    def __init__(self, pEnvConfig, pAssetGroupConfig, pLogger, pSpark, pAsset):
        self.aEnvConfig = pEnvConfig
        self.aAssetGroupConfig = pAssetGroupConfig
        self.aLogger = pLogger
        self.aSpark = pSpark
        self.aAsset = pAsset
        self.aInstructions = G_LoadTypeInstructions[pAsset[f'{AssetColumns.Load_Type_Code}']]

    def getNextVersionID(self):
        try:
            self.aLogger.debug("getNextVersionID.Start")
            df = self.aSpark.sql(
                "SELECT MAX(VERSION_ID) AS maxversion FROM " + self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            x = df.collect()[0][0]
            x = 0 if x is None else x + 1
            self.aLogger.debug("getNextVersionID.End")
            return x
        except Exception as ex:
            self.aLogger.error("getNextVersionID.Error: " + str(ex))
            raise Exception(ex)

    def getDeleteFieldAndValue(self):
        try:
            self.aLogger.debug("getDeleteFieldAndValue.Start")
            for dField in self.aAsset["Fields"]:
                if "Delete" in dField.get("ActionFlags", ""):
                    field_name = dField[f'{FieldColumns.Target_Field_Name}']
                    field_value = dField["ActionFlags"].split("=")[1]
                    self.aLogger.debug("getDeleteFieldAndValue.End")
                    return field_name, field_value
            self.aLogger.info("Could not find ActionFlags field with 'Delete'")
            return "", ""
        except Exception as ex:
            self.aLogger.error("getDeleteFieldAndValue.Error:" + str(ex))
            raise Exception(ex)

    def addVersionIDColumn(self, pDF):
        try:
            self.aLogger.debug("addVersionIDColumn.Start")
            df = pDF
            if not self.aInstructions["VersionColumn"]:
                self.aLogger.debug("addVersionIDColumn.Skipped")
                return df
            iNextVersion = self.getNextVersionID()
            df = df.withColumn("VERSION_ID", lit(iNextVersion))
            self.aLogger.debug("addVersionIDColumn.End")
            return df
        except Exception as ex:
            self.aLogger.error("addVersionIDColumn.Error: " + str(ex))
            raise Exception(ex)

    def addHashColumns(self, pDF):
        try:
            self.aLogger.debug("addHashColumns.Start")
            df = pDF
            if not self.aInstructions["HashColumns"]:
                self.aLogger.debug("addHashColumns.Skipped")
                return df
            lKeys = []
            lNonKeys = []
            for oF in self.aAsset["Fields"]:
                if oF[f'{FieldColumns.IsKey_Indicator}'] == "Y":
                    lKeys.append(coalesce(col(oF[f'{FieldColumns.Target_Field_Name}']), lit('NULL')))
                else:
                    lNonKeys.append(coalesce(col(oF[f'{FieldColumns.Target_Field_Name}']), lit('NULL')))
            df = df.withColumn("KEY_CHECKSUM_TXT", sha2(concat_ws('|', *lKeys), 256))
            df = df.withColumn("NON_KEY_CHECKSUM_TXT", sha2(concat_ws('|', *lNonKeys), 256))
            self.aLogger.debug("addHashColumns.End")
            return df
        except Exception as ex:
            self.aLogger.error("addHashColumns.Error: " + str(ex))
            raise Exception(ex)

