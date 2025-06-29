from pyspark.sql.functions import concat, col, lit, upper, lower, sha2, concat_ws, hash, to_date, date_format, \
    regexp_replace, udf, substring, round
from pyspark.sql.types import FloatType, IntegerType, StringType, DateType
from pyspark.sql.functions import *
from pyspark.sql.functions import sha2, concat_ws, hash
import datetime
import re
import DIFFieldConversion as DIFFieldConversion

from DatabaseConstants import TableNames, ProjectColumns, AssetGroupColumns, AssetColumns, FieldColumns, \
    IncrementalLoadColumns, AssetHistoryColumns, AssetStatusColumns, HistoricalLoadColumns, HolidayCalendarColumns
import importlib, sys

# This holds all the flags for each Load Type
G_TempSuffix = "_temp"

G_LoadTypeInstructions = {
    "T1-All-Replace": {
        "Description": "Get all data and completely replace what is there. No C Table.",
        "HashColumns": False, "VersionColumn": False, "FromToColumns": False,
        "CTable": False, "Delete": "All", "LatestView": True},
    "T1-Subset-Append": {
        "Description": "Get next block of data so just append to T table. No C Table.",
        "HashColumns": False, "VersionColumn": False, "FromToColumns": False,
        "CTable": False, "Delete": "None", "LatestView": False},
    "T1-Segment-Delete": {
        "Description": "Get full segment of data. Need to delete a segment. No C Table.",
        "HashColumns": False, "VersionColumn": False, "FromToColumns": False,
        "CTable": False, "Delete": "Hard.Segment", "LatestView": False},
    "T2-All-None-VersionID": {
        "Description": "Get full set of data; append w new version. No C Table.",
        "HashColumns": False, "VersionColumn": True, "FromToColumns": False,
        "CTable": False, "Delete": "None", "LatestView": False},
    "T2-All-None-FromTo": {
        "Description": "Get full data; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": True,
        "CTable": True, "Delete": "None", "LatestView": True},
    "T2-Subset-None-FromTo": {
        "Description": "Get full data; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": True,
        "CTable": True, "Delete": "None", "LatestView": False},
    "T2-All-SenseDelete-FromTo": {
        "Description": "Get full data; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": True,
        "CTable": True, "Delete": "MissingKeys", "LatestView": True},
    "T4-All-None": {
        "Description": "Get full data; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "None", "LatestView": False},
    "T4-All-SenseDelete": {
        "Description": "Get all data, missing key means delete; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "MissingKeys", "LatestView": False},
    "T4-Subset-SourceDeleteFlag": {
        "Description": "Get all data, missing key means delete; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "DeleteFlag", "LatestView": False},
    "T4-Subset-None": {
        "Description": "Get subset of data; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "None", "LatestView": False},
    "T4-Segment-Delete": {
        "Description": "Get subset of data; mark changes: NOT YET SUPPORTED",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "Soft.Segment", "LatestView": False},
}


class DIFProcessedTableHelper:
    def __init__(self, pEnvConfig, pAssetGroupConfig, pLogger, pSpark, pAsset):
        self.aEnvConfig = pEnvConfig
        self.aAssetGroupConfig = pAssetGroupConfig
        self.aLogger = pLogger
        self.aSpark = pSpark
        self.aAsset = pAsset
        self.aInstructions = G_LoadTypeInstructions[pAsset[f'{AssetColumns.Load_Type_Code}']]

    def dropProcessedTables(self):
        try:
            self.aLogger.info("dropProcessedTables.Start")
            G_SQL = "DROP TABLE IF EXISTS <TableName>;"

            sSQL = G_SQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            self.executeSQL(sSQL)

            sSQL = G_SQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
            self.executeSQL(sSQL)

            sSQL = G_SQL.replace("<TableName>",
                                 self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_"))
            self.executeSQL(sSQL)

            self.aLogger.info("dropProcessedTables.End")
        except Exception as ex:
            self.aLogger.error("dropProcessedTables.Error:" + str(ex))

    # T and C tables are identical
    def createProcessedTables(self):
        self.aLogger.debug("createProcessedTables.Start")
        self.createProcessedTable(self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
        self.createProcessedTable(self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
        if self.aInstructions["CTable"]:
            self.createProcessedTable(self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_"))
        self.aLogger.debug("createProcessedTables.End")

    def createOrReplaceViews(self):
        try:
            self.aLogger.info("updateLatestView.Start")
            self.aLogger.info("aInstructions:" + str(self.aInstructions))

            # Create V_ view (latest snapshot of T_)
            if self.aInstructions["LatestView"]:
                sSQL = "CREATE OR REPLACE VIEW <V_View> AS SELECT * FROM <TTable> <Where>"
                sWhere = "WHERE 1=1"
                sWhere += " AND CDC_LOAD_CODE != 'D'"
                if self.aInstructions["FromToColumns"]:
                    sWhere += " AND END_TS='2999-12-31'"
                sSQL = sSQL.replace("<Where>", sWhere)
                sSQL = sSQL.replace("<V_View>",
                                    self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".V_"))
                sSQL = sSQL.replace("<TTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
                self.aLogger.debug(sSQL)
                self.executeSQL(sSQL)
                self.aLogger.info("updateLatestView.V_View.Created")
            else:
                self.aLogger.info("updateLatestView.V_View.Skipped")

            createDecryptionView = self.shouldCreateDecryptionView()

            # Create TV_ view (decrypted view on T_ table)
            if createDecryptionView:
                sSelectClause = self.getDecryptedSelectClause()
                sViewName = self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".TV_")
                sTTableName = self.aAsset[f'{AssetColumns.Processed_Table_Name}']

                sSQL = f"""
                CREATE OR REPLACE VIEW {sViewName} AS 
                SELECT {sSelectClause} FROM {sTTableName} T
                WHERE T.CDC_LOAD_CODE != 'D'
                """
                if self.aInstructions["FromToColumns"]:
                    sSQL += " AND T.END_TS = '2999-12-31'"
                self.executeSQL(sSQL)
                self.aLogger.info("updateLatestView.TV_View.Created")

            # Create CV_ view (decrypted view on C_ table if CTable is enabled)
            if self.aInstructions["CTable"] and createDecryptionView:
                self.aLogger.info("updateLatestView.CV_View.Start")
                sSelectClause = self.getDecryptedSelectClause()
                sViewName = self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".CV_")
                sCTableName = self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_")

                sSQL = f"""
                CREATE OR REPLACE VIEW {sViewName} AS 
                SELECT {sSelectClause} FROM {sCTableName} C
                WHERE C.CDC_LOAD_CODE != 'X'
                """
                self.executeSQL(sSQL)
                self.aLogger.info("updateLatestView.CV_View.Created")

            self.aLogger.info("updateLatestView.End")
        except Exception as ex:
            self.aLogger.error("updateLatestView.Error: " + str(ex))
            raise Exception(str(ex))

    def createProcessedTable(self, pTableName):
        try:
            self.aLogger.info("createProcessedTable.Start:" + pTableName)
            sSQL = "CREATE TABLE IF NOT EXISTS <TableName> (<ColumnList>)"
            sSQL = sSQL.replace("<TableName>", pTableName)

            sColumnList = self.getColumns()

            # Add system columns
            if self.aInstructions["HashColumns"]:
                sColumnList += ", KEY_CHECKSUM_TXT STRING, NON_KEY_CHECKSUM_TXT STRING"
            if self.aInstructions["VersionColumn"]:
                sColumnList += ", VERSION_ID INT"
            if self.aInstructions["FromToColumns"]:
                sColumnList += ", START_TS STRING, END_TS STRING"

                # ALWAYS ADDED
            sColumnList += ", BusinessDate DATE, DIFSourceFile STRING, DIFLoadDate TIMESTAMP, LOAD_TS TIMESTAMP, CDC_LOAD_CODE STRING"
            for dField in self.aAsset["Fields"]:
                if 'Encrypt' in dField.get(f'{FieldColumns.Data_Protection_Code}', ''):
                    sColumnList += ", DP_Encrypter_Passphrase_Version STRING"
                    break

            sSQL = sSQL.replace("<ColumnList>", sColumnList)

            # Get partition fields with types
            lPartitionFields = []
            for dField in self.aAsset["Fields"]:
                if dField.get("Partitioned_Indicator") == "Y":
                    lPartitionFields.append(dField[f'{FieldColumns.Target_Field_Name}'])
            self.aLogger.info("lPartitionFields: " + str(lPartitionFields))

            if lPartitionFields:
                sSQL += " PARTITIONED BY (" + ", ".join(lPartitionFields) + ")"

            self.executeSQL(sSQL)
            self.aLogger.info("createProcessedTable.End")
        except Exception as ex:
            self.aLogger.error("createProcessedTable.Error: " + str(ex))
            raise Exception(str(ex))

    # This needs to build an Initial temp table
    def populateTempTable(self, pTimeStamp):
        try:
            self.aLogger.info("populateTempTable.Start")
            dResults = {"Continue": True, "Message": ""}
            sModuleName = self.aAssetGroupConfig["AssetGroupName"]
            self.truncateTable(self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
            dfRaw = self.getRawDataFrame(pTimeStamp)
            excludedFields = [
                dField[f'{FieldColumns.Target_Field_Name}']
                for dField in self.aAsset["Fields"]
                if dField.get(f'{FieldColumns.Exclude_From_Processed_Indicator}') == "Y"
            ]

            # Drop the excluded fields from dfRaw
            if excludedFields:
                self.aLogger.info(f"Excluding fields: {excludedFields}")
                dfRaw = dfRaw.drop(*excludedFields)

            try:
                folder_path = self.aAsset["Custom_Exits_Module_Name"]  # Path to your folder
                if folder_path not in sys.path:
                    sys.path.append(folder_path)
                PackageExits = importlib.import_module(sModuleName)
                sModule = getattr(PackageExits, sModuleName)
                dResults = sModule(self.aAsset, self.aAssetGroupConfig, self.aLogger).rawToProcessed(
                    self.aAssetGroupConfig, self.aLogger, self.aAsset, dfRaw)
                # print("module found for:",dResults)
                self.aLogger.info(f'Module found for: {dResults}')

            except Exception as ex:
                # print("module found for: " + sModuleName,ex)
                self.aLogger.error(" No Module found for: " + str(sModuleName))

            # If there are failed tests, log the error and return the exception to the main class
            CustomExitResult = [
                test for test in dResults.get('TestResults', []) if test.get('Continue') == False
            ]

            # If there are failed tests, log the error and return the exception to the main class

            if CustomExitResult:
                return dResults

            oFC = DIFFieldConversion.DIFFieldConversion(self.aEnvConfig, self.aLogger, self.aSpark, self.aAsset)
            dfRaw = oFC.applyConversions(dfRaw)
            dfAdjusted = self.getAdjustedRawDataFrame(dfRaw)
            dfAdjusted.write.mode("append").saveAsTable(
                self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
            self.aLogger.info("populateTempTable.End")
            return dResults
        except Exception as ex:
            self.aLogger.error("populateTempTable.Error:" + str(ex))
            raise Exception(ex)

    # Mark temp table entries with NC=NoChange, U or D
    def updateTempTable(self):
        try:
            self.aLogger.info("updateTempTable.Start")
            self.updateTempTableNoChange()
            self.updateTempTableUpdate()
            self.updateTempTableSoftDelete()
            # self.updateTempTableDelete()
            self.aLogger.info("updateTempTable.End")
        except Exception as ex:
            self.aLogger.info("updateTempTable.Error:" + str(ex))
            raise Exception(ex)

    def updateTempTableNoChange(self):
        try:
            self.aLogger.info("updateTempTableNoChange.Start")
            if not self.aInstructions["HashColumns"]:
                self.aLogger.info("updateTempTableNoChange.Skipping")
                return

            # mark unchanged temp records
            sSQL = "Update <TempTableName> set CDC_LOAD_CODE='NC' "
            sSQL = sSQL + " where concat(KEY_CHECKSUM_TXT,NON_KEY_CHECKSUM_TXT) in "
            sSQL = sSQL + " (Select concat(KEY_CHECKSUM_TXT,NON_KEY_CHECKSUM_TXT) "
            sSQL = sSQL + " from <TableName> Where CDC_LOAD_CODE<> 'D' <Where>) "
            sWhere = ""
            if self.aInstructions["FromToColumns"]:
                sWhere = sWhere + " and END_TS='2999-12-31'"

            sSQL = sSQL.replace("<TempTableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
            sSQL = sSQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            sSQL = sSQL.replace("<Where>", sWhere)

            self.executeSQL(sSQL)
            self.aLogger.info("updateTempTableNoChange.End")
        except Exception as ex:
            self.aLogger.info("updateTempTableNoChange.Error:" + str(ex))
            raise Exception(ex)

    def updateTempTableUpdate(self):
        try:
            self.aLogger.info("updateTempTableUpdate.Start")
            if not self.aInstructions["HashColumns"]:
                self.aLogger.info("updateTempTableChange.Skipping")
                return

            # mark unchanged temp records
            sSQL = "Update <TempTableName> set CDC_LOAD_CODE='U' "
            sSQL = sSQL + " where KEY_CHECKSUM_TXT in "
            sSQL = sSQL + "(Select KEY_CHECKSUM_TXT from <TableName> Where <Where>) "
            sSQL = sSQL + " And concat(KEY_CHECKSUM_TXT,NON_KEY_CHECKSUM_TXT) not in "
            sSQL = sSQL + " (Select concat(KEY_CHECKSUM_TXT,NON_KEY_CHECKSUM_TXT) from <TableName> Where <Where>) "
            sWhere = " 1=1 and CDC_LOAD_CODE <> 'D'"
            if self.aInstructions["FromToColumns"]:
                sWhere = sWhere + " and END_TS='2999-12-31'"

            sSQL = sSQL.replace("<TempTableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
            sSQL = sSQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            sSQL = sSQL.replace("<Where>", sWhere)

            self.executeSQL(sSQL)
            self.aLogger.info("updateTempTableUpdate.End")
        except Exception as ex:
            self.aLogger.info("updateTempTableUpdate.Error:" + str(ex))
            raise Exception(ex)

    def updateTempTableSoftDelete(self):
        try:
            self.aLogger.info("updateTempTableSoftDelete.Start")
            if self.aInstructions["Delete"] != "DeleteFlag":
                self.aLogger.info("updateTempTableSoftDelete.Skipping")
                return

            sFieldName, sFieldValue = self.getDeleteFieldAndValue()
            # Temp records where Field=X must be marked as CDC_LOAD_CODE = 'D'
            sSQL = "Update <TempTableName> set CDC_LOAD_CODE='D' where <Field>='<FieldValue>'"
            sSQL = sSQL.replace("<TempTableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
            sSQL = sSQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            sSQL = sSQL.replace("<Field>", sFieldName)
            sSQL = sSQL.replace("<FieldValue>", sFieldValue)

            self.executeSQL(sSQL)
            self.aLogger.info("updateTempTableSoftDelete.End")
        except Exception as ex:
            self.aLogger.info("updateTempTableSoftDelete.Error:" + str(ex))
            raise Exception(ex)

    def handleHardDeleteFromTTable(self):
        try:
            self.aLogger.info("deleteFromTTable.Start")
            if self.aInstructions["Delete"] == "All":
                self.truncateTable(self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            if self.aInstructions["Delete"] == "None":
                pass
            if self.aInstructions["Delete"] == "Hard.Segment":
                self.truncateTableSegment()
            if self.aInstructions["Delete"] == "MissingKeys":
                pass
                # NEED TO SUPPORT

            self.aLogger.info("deleteFromTTable.End")
        except Exception as ex:
            self.aLogger.error("deleteFromTTable.Error:" + str(ex))
            raise Exception(ex)

    def applyTempRecordsToTTable(self, pTimeStamp):
        try:
            self.aLogger.info("applyTempRecordsToTTable.Start")

            # From/To and Updates
            if self.aInstructions["FromToColumns"]:
                sSQL = "Update <TTable> set CDC_LOAD_CODE = 'U',END_TS='<EndDate>',LOAD_TS=cast('<TimeStamp> EST' as TimeStamp)"
                sSQL = sSQL + " Where CDC_LOAD_CODE='I' and END_TS='2999-12-31' "
                sSQL = sSQL + " And KEY_CHECKSUM_TXT in "
                sSQL = sSQL + "(Select KEY_CHECKSUM_TXT From <TempTable> Where CDC_LOAD_CODE in ('U') )"
                sSQL = sSQL.replace("<TTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
                sSQL = sSQL.replace("<TempTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
                sSQL = sSQL.replace("<EndDate>", self.aAsset["BusinessDate"])
                sSQL = sSQL.replace("<TimeStamp>", pTimeStamp)
                self.executeSQL(sSQL)

            # FromTo + Deletes
            if self.aInstructions["FromToColumns"]:
                sSQL = "Update <TTable> set CDC_LOAD_CODE = 'D',END_TS='<EndDate>',LOAD_TS=cast('<TimeStamp> EST' as TimeStamp)"
                sSQL = sSQL + " Where CDC_LOAD_CODE='I' and END_TS='2999-12-31' "
                sSQL = sSQL + " And KEY_CHECKSUM_TXT in "
                sSQL = sSQL + "(Select KEY_CHECKSUM_TXT From <TempTable> Where CDC_LOAD_CODE in ('D') )"
                sSQL = sSQL.replace("<TTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
                sSQL = sSQL.replace("<TempTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
                sSQL = sSQL.replace("<EndDate>", self.aAsset["BusinessDate"])
                sSQL = sSQL.replace("<TimeStamp>", pTimeStamp)
                self.executeSQL(sSQL)

            # HashColumns: Updates and Deletes must be physically deleted
            if not self.aInstructions["FromToColumns"] and self.aInstructions["HashColumns"]:
                sSQL = "Delete from <TTable> "
                sSQL = sSQL + "Where KEY_CHECKSUM_TXT in "
                sSQL = sSQL + "(Select KEY_CHECKSUM_TXT From <TempTable> Where CDC_LOAD_CODE in ('U','D') )"
                sSQL = sSQL.replace("<TTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
                sSQL = sSQL.replace("<TempTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
                self.executeSQL(sSQL)

            # Missing Keys: Mark as Deleted
            if self.aInstructions["Delete"] == "MissingKeys":
                sSQL = "Update <TTable> set CDC_LOAD_CODE='D', LOAD_TS=cast('<TimeStamp> EST' as TimeStamp)"
                if self.aInstructions["FromToColumns"]:
                    sSQL = sSQL + ",END_TS='<EndDate>'"
                sSQL = sSQL + " where KEY_CHECKSUM_TXT not in "
                sSQL = sSQL + " (Select KEY_CHECKSUM_TXT from <TempTable>) "
                sSQL = sSQL.replace("<TTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
                sSQL = sSQL.replace("<TempTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
                sSQL = sSQL.replace("<TimeStamp>", pTimeStamp)
                sSQL = sSQL.replace("<EndDate>", self.aAsset["BusinessDate"])
                self.executeSQL(sSQL)

            # Finally get all the new data into the T Table
            sSQL = "Insert into <TTable> "
            sSQL = sSQL + "(Select * from <TempTable> where CDC_LOAD_CODE in ('U','I','D'))"
            sSQL = sSQL.replace("<TTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            sSQL = sSQL.replace("<TempTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
            self.executeSQL(sSQL)

            self.aLogger.info("applyTempRecordsToTTable.End")
        except Exception as ex:
            self.aLogger.error("applyTempRecordsToTTable.Error:" + str(ex))
            raise Exception(ex)

    # Simply add to C table any record in T table that is newly changed
    def addToCTable(self, pTimeStamp):
        try:
            self.aLogger.info("addToCTable.Start")
            if not self.aInstructions["CTable"]:
                self.aLogger.info("addToCTable.End.NotNeeded")
                return
            sSQL = "Insert into <CTable> select * from <TTable> where LOAD_TS=cast('<TimeStamp> EST' as TimeStamp)"
            sSQL = sSQL.replace("<CTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_"))
            sSQL = sSQL.replace("<TTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            sSQL = sSQL.replace("<TimeStamp>", pTimeStamp)
            self.executeSQL(sSQL)
            self.aLogger.info("addToCTable.End")
        except Exception as ex:
            self.aLogger.error("addToCTable.Error:" + str(ex))
            raise Exception(ex)

    #
    # PRIVATE FUNCTIONS
    #
    # Returns raw data with CDC Code = I and TimeStamp
    def getRawDataFrame(self, pTimeStamp):
        try:
            self.aLogger.info("DIFTableHelper.getRawDataFrame.25.02.26")
            self.aLogger.debug("Refresh the table because the select below is not working")
            self.aSpark.sql("Refresh table " + self.aAsset[AssetColumns.Raw_Table_Name])

            # Now select the data from Raw
            sSQL = "Select *, 'I' as CDC_LOAD_CODE, cast('<TimeStamp> EST' as TimeStamp) as LOAD_TS from <RawTable> "
            sSQL = sSQL + "where DIFSourceFile='<SourceFile>' and BusinessDate='<BusinessDate>' "
            sSQL = sSQL.replace("<TimeStamp>", pTimeStamp)
            sSQL = sSQL.replace("<RawTable>", self.aAsset[f'{AssetColumns.Raw_Table_Name}'])
            sSQL = sSQL.replace("<SourceFile>", self.aAsset["IngestFile"])
            # sSQL = sSQL.replace("<SourceFile>", self.aAsset["ShortFile"])
            sSQL = sSQL.replace("<BusinessDate>", self.aAsset["BusinessDate"])

            self.aLogger.debug(sSQL)
            df = self.aSpark.sql(sSQL)
            self.aLogger.debug("This is the data from the Raw table to be processed")
            print(df)
            df.show()
            self.aLogger.info("getRawDataFrame.End.25.02.25")
            return df

        except Exception as ex:
            self.aLogger.error("getDataFrameNew_Default.Error:" + str(ex))
            raise Exception(ex)

    # Takes Raw DAtaFrame and adds columns as needed
    # VERSION_ID
    def getAdjustedRawDataFrame(self, pDF):
        try:
            self.aLogger.debug("getAdjustedRawDataFrame.Start")
            df = pDF
            df = self.addVersionIDColumn(df)
            df = self.addHashColumns(df)
            df = self.addFromToColumns(df)
            self.aLogger.debug("getAdjustedRawDataFrame.Start")
            return df
        except Exception as ex:
            self.aLogger.debug("getAdjustedRawDataFrame.Error:" + str(ex))
            raise Exception(ex)

    def addVersionIDColumn(self, pDF):
        try:
            self.aLogger.debug("addVersionID.Start")
            df = pDF
            if not self.aInstructions["VersionColumn"]:
                self.aLogger.debug("addVersionIDColumn.Skipping")
                return df
            iNextVersion = self.getNextVersionID()
            df = df.withColumn('VERSION_ID', lit(iNextVersion))
            self.aLogger.debug("addVersionID.End")
            return df
        except Exception as ex:
            self.aLogger.error("addVersionID.Error:" + str(ex))
            raise Exception(ex)

    def addFromToColumns(self, pDF):
        try:
            self.aLogger.debug("addFromToColumns.Start")
            df = pDF
            if not self.aInstructions["FromToColumns"]:
                self.aLogger.debug("addFromToColumns.Skipping")
                return df

            sLater = "2999-12-31"
            df = df.withColumn('START_TS', lit(self.aAsset["BusinessDate"]))
            df = df.withColumn('END_TS', lit(sLater))
            self.aLogger.debug("addFromToColumns.End")
            return df
        except Exception as ex:
            self.aLogger.error("addFromToColumns.Error:" + str(ex))
            raise Exception(ex)

    # Adds KEY_CHECKSUM_TXT and NON_KEY_CHECKSUM_TXT columns if needed
    def addHashColumns(self, pDF):
        try:
            self.aLogger.debug("addHashColumns.Start")
            df = pDF
            if not self.aInstructions["HashColumns"]:
                self.aLogger.debug("addHashColumns.Skipping")
                return df

            lKeys = []
            lNonKeys = []
            for oF in self.aAsset["Fields"]:
                if oF[f'{FieldColumns.IsKey_Indicator}'] == "Y":
                    lKeys.append(oF[f'{FieldColumns.Target_Field_Name}'])
                else:
                    lNonKeys.append(oF[f'{FieldColumns.Target_Field_Name}'])
            df = df.withColumn('KEY_CHECKSUM_TXT', sha2(concat_ws('|', *lKeys), 256))
            df = df.withColumn('NON_KEY_CHECKSUM_TXT', sha2(concat_ws('|', *lNonKeys), 256))
            self.aLogger.debug("addHashColumns.End")
            return df
        except Exception as ex:
            self.aLogger.error("addHashColumns.Error:" + str(ex))
            raise Exception(ex)

    def getNextVersionID(self):
        try:
            self.aLogger.debug("getNextVersionID.Start")
            df = self.aSpark.sql(
                "select max(VERSION_ID) as maxversion from " + self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            x = df.collect()[0][0]
            if x == None:
                x = 0
            x = x + 1
            self.aLogger.debug("getNextVersionID.End")
            return x
        except Exception as ex:
            self.aLogger.debug("getNextVersionID.Error:" + str(ex))
            raise Exception(ex)

    def truncateTable(self, pTableName):
        try:
            self.aLogger.info("truncateTable.Start")
            G_SQL = "Delete from <TableName>;"
            sSQL = G_SQL.replace("<TableName>", pTableName)
            self.executeSQL(sSQL)
            self.aLogger.info("truncateTable.End")
        except Exception as ex:
            self.aLogger.error("truncateTable.Error:" + str(ex))
            raise Exception(ex)

    def getSnapshotField(self):
        self.aLogger.info("getSnapshotField.Start.25.02.26")
        for sField in self.aAsset["Fields"]:
            if sField["Snapshot_Indicator"] == "Y":
                return sField[f'{FieldColumns.Target_Field_Name}']
        raise Exception("No snapshot field was found")

    def executeSQL(self, pSQL):
        self.aLogger.debug(pSQL)
        self.aSpark.sql(pSQL)

    # Find Field and Value that indicates a Deletion record in Raw table
    def getDeleteFieldAndValue(self):
        try:
            self.aLogger.debug("getDeleteFieldAndValue.Start")
            for dField in self.aAsset["Fields"]:
                if "Delete" in dField["ActionFlags"]:
                    self.aLogger.debug("getDeleteFieldAndValue.End")
                    return dField[f'{FieldColumns.Target_Field_Name}'], dField["ActionFlags"].split("=")[1]

            self.aLogger.info("Could not find ActionFlags field")
            return "", ""

        except Exception as ex:
            self.aLogger.error("getDeleteFieldAndValue.Error:" + str(ex))
            raise Exception(ex)

    def getColumns(self):
        sColumnList = ""

        # Sorted fields based on Field_Sequence_Number
        sorted_fields = sorted(self.aAsset["Fields"], key=lambda x: x["Field_Sequence_Number"])

        for dField in sorted_fields:
            if dField.get(f'{FieldColumns.Exclude_From_Processed_Indicator}') == "Y":
                continue

            if sColumnList:
                sColumnList += ", "

            sColumnList = sColumnList + dField[f'{FieldColumns.Target_Field_Name}'] + " " + dField[
                f'{FieldColumns.Target_Data_Type_Code}']

        return sColumnList

    def getDecryptedSelectClause(self):
        try:
            self.aLogger.debug("getDecryptedSelectClause.Start")
            lSelectColumns = []

            for dField in self.aAsset["Fields"]:
                sFieldName = dField[f'{FieldColumns.Target_Field_Name}']

                if 'Encrypt' in dField.get(f'{FieldColumns.Data_Protection_Code}', ''):
                    lSelectColumns.append(
                        f"db_security.f_dp_decrypt({sFieldName}, DP_Encrypter_Passphrase_Version) AS {sFieldName}"
                    )
                else:
                    lSelectColumns.append(sFieldName)

            # Add system metadata columns
            lSelectColumns += ["BusinessDate", "DIFSourceFile", "DIFLoadDate", "LOAD_TS", "CDC_LOAD_CODE"]

            sSelectClause = ", ".join(lSelectColumns)
            self.aLogger.debug("getDecryptedSelectClause.End")
            return sSelectClause

        except Exception as ex:
            self.aLogger.error("getDecryptedSelectClause.Error: " + str(ex))
            raise Exception(str(ex))

    def shouldCreateDecryptionView(self):
        try:
            self.aLogger.debug("shouldCreateDecryptionView.Start")
            for dField in self.aAsset["Fields"]:
                if 'Encrypt' in dField.get(f'{FieldColumns.Data_Protection_Code}', ''):
                    return True
            return False
        except Exception as ex:
            self.aLogger.error("shouldCreateDecryptionView.Error: " + str(ex))
            raise Exception(str(ex))