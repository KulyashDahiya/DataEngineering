from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import importlib, sys

from DatabaseConstants import TableNames, ProjectColumns, AssetGroupColumns, AssetColumns, FieldColumns, \
    IncrementalLoadColumns, AssetHistoryColumns, AssetStatusColumns, HistoricalLoadColumns, HolidayCalendarColumns
import DIFFieldConversion

G_LoadTypeInstructions = {
    "T1-All-Replace": {"Description": "Get all data and completely replace what is there. No C Table.",
                       "HashColumns": False, "VersionColumn": False, "FromToColumns": False, "CTable": False,
                       "Delete": "All", "LatestView": True, "Merge": False},
    "T1-Subset-Append": {"Description": "Append to T table. No C Table.", "HashColumns": False, "VersionColumn": False,
                         "FromToColumns": False, "CTable": False, "Delete": "None", "LatestView": False,
                         "Merge": False},
    "T1-Segment-Delete": {"Description": "Delete segment. No C Table.", "HashColumns": False, "VersionColumn": False,
                          "FromToColumns": False, "CTable": False, "Delete": "Hard.Segment", "LatestView": False,
                          "Merge": False},
    "T2-All-None-VersionID": {"Description": "Append with version ID. No C Table.", "HashColumns": False,
                              "VersionColumn": True, "FromToColumns": False, "CTable": False, "Delete": "None",
                              "LatestView": False, "Merge": False},
    "T2-All-None-FromTo": {"Description": "Full data; changes with From-To columns", "HashColumns": True,
                           "VersionColumn": False, "FromToColumns": True, "CTable": True, "Delete": "None",
                           "LatestView": True, "Merge": False},
    "T2-Subset-None-FromTo": {"Description": "Subset data; mark changes", "HashColumns": True, "VersionColumn": False,
                              "FromToColumns": True, "CTable": True, "Delete": "None", "LatestView": False,
                              "Merge": False},
    "T2-All-SenseDelete-FromTo": {"Description": "Full data; mark changes, sense delete", "HashColumns": True,
                                  "VersionColumn": False, "FromToColumns": True, "CTable": True,
                                  "Delete": "MissingKeys", "LatestView": True, "Merge": False},
    "T4-All-None": {"Description": "Full data; mark changes", "HashColumns": True, "VersionColumn": False,
                    "FromToColumns": False, "CTable": True, "Delete": "None", "LatestView": False, "Merge": True},
    "T4-All-SenseDelete": {"Description": "Full data; mark changes, soft delete missing keys", "HashColumns": True,
                           "VersionColumn": False, "FromToColumns": False, "CTable": True, "Delete": "MissingKeys",
                           "LatestView": False, "Merge": True},
    "T4-Subset-SourceDeleteFlag": {"Description": "Subset; delete flagged records", "HashColumns": True,
                                   "VersionColumn": False, "FromToColumns": False, "CTable": True,
                                   "Delete": "DeleteFlag", "LatestView": False, "Merge": True},
    "T4-Subset-None": {"Description": "Subset of data; mark changes", "HashColumns": True, "VersionColumn": False,
                       "FromToColumns": False, "CTable": True, "Delete": "None", "LatestView": False, "Merge": True},
    "T4-Segment-Delete": {"Description": "Subset data; delete segment", "HashColumns": True, "VersionColumn": False,
                          "FromToColumns": False, "CTable": True, "Delete": "Soft.Segment", "LatestView": False,
                          "Merge": True}
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
            baseTable = self.aAsset[f'{AssetColumns.Processed_Table_Name}']
            cTable = baseTable.replace(".T_", ".C_")

            self.executeSQL(f"DROP TABLE IF EXISTS {baseTable}")
            if self.aInstructions["CTable"]:
                self.executeSQL(f"DROP TABLE IF EXISTS {cTable}")

            self.aLogger.info("dropProcessedTables.End")
        except Exception as ex:
            self.aLogger.error("dropProcessedTables.Error: " + str(ex))
            raise Exception(ex)

    # T and C tables are identical
    def createProcessedTables(self):
        self.aLogger.debug("createProcessedTables.Start")
        self.createProcessedTable(self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
        if self.aInstructions["CTable"]:
            self.createProcessedTable(self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_"))
        self.aLogger.debug("createProcessedTables.End")

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

    # Adds KEY_CHECKSUM_TXT and NON_KEY_CHECKSUM_TXT columns if needed
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

    def addFromToColumns(self, pDF):
        try:
            self.aLogger.debug("addFromToColumns.Start")
            df = pDF
            if not self.aInstructions["FromToColumns"]:
                self.aLogger.debug("addFromToColumns.Skipped")
                return df

            sLater = "2999-12-31"
            df = df.withColumn("START_TS", lit(self.aAsset["BusinessDate"]))
            df = df.withColumn("END_TS", lit(sLater))
            self.aLogger.debug("addFromToColumns.End")
            return df
        except Exception as ex:
            self.aLogger.error("addFromToColumns.Error: " + str(ex))
            raise Exception(ex)

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

    def createOrReplaceViews(self):
        try:
            self.aLogger.info("createOrReplaceViews.Start")
            tTable = self.aAsset[f'{AssetColumns.Processed_Table_Name}']
            vView = tTable.replace(".T_", ".V_")

            if self.aInstructions["LatestView"]:
                sWhere = "WHERE CDC_LOAD_CODE != 'D'"
                if self.aInstructions["FromToColumns"]:
                    sWhere += " AND END_TS = '2999-12-31'"

                sSQL = f"""
                    CREATE OR REPLACE VIEW {vView} AS
                    SELECT * FROM {tTable} {sWhere}
                    """
                self.executeSQL(sSQL)
                self.aLogger.info("V_ View created: " + vView)

            createDecryptionView = self.shouldCreateDecryptionView()
            # Create TV_ view (decrypted view on T_ table)
            if createDecryptionView:
                sSelectClause = self.getDecryptedSelectClause()
                tvView = tTable.replace(".T_", ".TV_")
                sSQL = f"""
                    CREATE OR REPLACE VIEW {tvView} AS
                    SELECT {sSelectClause} FROM {tTable} WHERE CDC_LOAD_CODE != 'D'
                    """
                if self.aInstructions["FromToColumns"]:
                    sSQL += " AND END_TS = '2999-12-31'"
                self.executeSQL(sSQL)
                self.aLogger.info("TV_ View created: " + tvView)

                # Create CV_ view (decrypted view on C_ table)
                if self.aInstructions["CTable"]:
                    cTable = tTable.replace(".T_", ".C_")
                    cvView = tTable.replace(".T_", ".CV_")
                    sSQL = f"""
                        CREATE OR REPLACE VIEW {cvView} AS
                        SELECT {sSelectClause} FROM {cTable} WHERE CDC_LOAD_CODE != 'X'
                        """
                    self.executeSQL(sSQL)
                    self.aLogger.info("CV_ View created: " + cvView)

            self.aLogger.info("createOrReplaceViews.End")
        except Exception as ex:
            self.aLogger.error("createOrReplaceViews.Error: " + str(ex))
            raise Exception(ex)

    def getSnapshotField(self):
        try:
            self.aLogger.info("getSnapshotField.Start")
            for dField in self.aAsset["Fields"]:
                if dField.get("Snapshot_Indicator") == "Y":
                    return dField[f'{FieldColumns.Target_Field_Name}']
            raise Exception("No snapshot field found with Snapshot_Indicator = 'Y'")
        except Exception as ex:
            self.aLogger.error("getSnapshotField.Error: " + str(ex))
            raise Exception(ex)

    def executeSQL(self, pSQL):
        try:
            self.aLogger.debug("Executing SQL: " + pSQL)
            self.aSpark.sql(pSQL)
        except Exception as ex:
            self.aLogger.error("executeSQL.Error: " + str(ex))
            raise Exception(ex)

    # Find Field and Value that indicates a Deletion record in Raw table
    def getDeleteFieldAndValue(self):
        try:
            self.aLogger.debug("getDeleteFieldAndValue.Start")
            for dField in self.aAsset["Fields"]:
                if "Delete" in dField.get("ActionFlags", ""):
                    fieldName = dField[f'{FieldColumns.Target_Field_Name}']
                    fieldValue = dField["ActionFlags"].split("=")[1]
                    self.aLogger.debug("getDeleteFieldAndValue.End")
                    return fieldName, fieldValue
            self.aLogger.info("Could not find ActionFlags field with 'Delete'")
            return "", ""
        except Exception as ex:
            self.aLogger.error("getDeleteFieldAndValue.Error:" + str(ex))
            raise Exception(ex)

    def updateTempRecordsToTTable(self, pTimeStamp):
        mainTable = self.aAsset[f'{AssetColumns.Processed_Table_Name}']
        dfRaw = self.getRawDataFrame(pTimeStamp)
        dfIncoming = self.getAdjustedRawDataFrame(dfRaw)

        if self.aInstructions["Delete"] == "All":
            self.aLogger.info("DeleteMode: All. Truncating and recreating table.")
            self.truncateTable(mainTable)
            dfIncoming = dfIncoming.select(*self.aSpark.table(mainTable).columns)  # Schema Alignment
            dfIncoming.write.mode("append").saveAsTable(mainTable)
        elif self.aInstructions["Merge"]:
            self.mergeAndTrackChangesWithDelta(dfIncoming, pTimeStamp)
        else:
            self.appendAndTrackChangesWithoutMerge(dfIncoming, pTimeStamp)

    def getAdjustedRawDataFrame(self, dfRaw):
        try:
            self.aLogger.debug("getAdjustedRawDataFrame.Start")
            df = dfRaw

            excludedFields = [
                dField[f'{FieldColumns.Target_Field_Name}']
                for dField in self.aAsset["Fields"]
                if dField.get(f'{FieldColumns.Exclude_From_Processed_Indicator}') == "Y"
            ]
            if excludedFields:
                self.aLogger.info(f"Excluding fields: {excludedFields}")
                df = df.drop(*excludedFields)

            try:
                sModuleName = self.aAssetGroupConfig["AssetGroupName"]
                folder_path = self.aAsset["Custom_Exits_Module_Name"]
                if folder_path not in sys.path:
                    sys.path.append(folder_path)
                PackageExits = importlib.import_module(sModuleName)
                sModule = getattr(PackageExits, sModuleName)
                dResults = sModule(self.aAsset, self.aAssetGroupConfig, self.aLogger).rawToProcessed(
                    self.aAssetGroupConfig, self.aLogger, self.aAsset, df)
                self.aLogger.info(f"Module found for: {dResults}")
            except Exception as ex:
                self.aLogger.error("No Module found for: " + str(sModuleName))
                dResults = {"TestResults": []}

            CustomExitResult = [
                test for test in dResults.get('TestResults', []) if test.get('Continue') == False
            ]
            if CustomExitResult:
                raise Exception("Custom exit test failed")

            oFC = DIFFieldConversion.DIFFieldConversion(self.aEnvConfig, self.aLogger, self.aSpark, self.aAsset)
            df = oFC.applyConversions(df)

            df = self.addVersionIDColumn(df)
            df = self.addHashColumns(df)
            df = self.addFromToColumns(df)
            self.aLogger.debug("getAdjustedRawDataFrame.End")
            return df
        except Exception as ex:
            self.aLogger.error("getAdjustedRawDataFrame.Error: " + str(ex))
            raise Exception(ex)

    def applyTempRecordsToTTable(self, pTimeStamp):
        try:
            self.aLogger.info("applyTempRecordsToTTable.Start")

            # Get the incoming adjusted data
            dfRaw = self.getRawDataFrame(pTimeStamp)
            dfIncoming = self.getAdjustedRawDataFrame(dfRaw)

            # Get the main processed table (T Table)
            mainTable = self.aAsset[f'{AssetColumns.Processed_Table_Name}']
            dfTarget = self.aSpark.table(mainTable)

            # Handle From/To and Updates (if applicable)
            if self.aInstructions["FromToColumns"]:
                # Mark records for update where END_TS = '2999-12-31'
                dfUpdates = dfIncoming.join(dfTarget, on="KEY_CHECKSUM_TXT", how="inner") \
                    .filter((dfTarget["END_TS"] == "2999-12-31") & (dfIncoming["CDC_LOAD_CODE"] == "I"))

                dfUpdates = dfUpdates.withColumn("CDC_LOAD_CODE", lit("U")) \
                    .withColumn("END_TS", lit(self.aAsset["BusinessDate"])) \
                    .withColumn("LOAD_TS", to_timestamp(lit(pTimeStamp), 'yyyy-MM-dd HH:mm:ss'))

                # Update the T Table with CDC_LOAD_CODE='U'
                dfTarget = dfTarget.subtract(dfUpdates)  # Remove old data that's being updated
                dfTarget = dfTarget.union(dfUpdates)  # Add updated data

            # Handle From/To + Deletes (if applicable)
            if self.aInstructions["FromToColumns"]:
                # Mark records for deletion where CDC_LOAD_CODE = 'I' and END_TS = '2999-12-31'
                dfDeletes = dfIncoming.join(dfTarget, on="KEY_CHECKSUM_TXT", how="left_anti") \
                    .filter(dfTarget["CDC_LOAD_CODE"] == "I")

                dfDeletes = dfDeletes.withColumn("CDC_LOAD_CODE", lit("D")) \
                    .withColumn("END_TS", lit(self.aAsset["BusinessDate"])) \
                    .withColumn("LOAD_TS", to_timestamp(lit(pTimeStamp), 'yyyy-MM-dd HH:mm:ss'))

                # Remove deleted records from T Table
                dfTarget = dfTarget.subtract(dfDeletes)  # Remove records to be deleted
                dfTarget = dfTarget.union(dfDeletes)  # Add deleted records

            # Handle HashColumns: Updates and Deletes must be physically deleted (if applicable)
            if not self.aInstructions["FromToColumns"] and self.aInstructions["HashColumns"]:
                dfToDelete = dfIncoming.join(dfTarget, on="KEY_CHECKSUM_TXT", how="inner") \
                    .filter(dfIncoming["CDC_LOAD_CODE"].isin("U", "D"))

                dfTarget = dfTarget.subtract(dfToDelete)  # Remove deleted or updated records

            # Handle Missing Keys: Mark as Deleted (if applicable)
            if self.aInstructions["Delete"] == "MissingKeys":
                missingKeys = dfTarget.join(dfIncoming, on="KEY_CHECKSUM_TXT", how="left_anti")

                missingKeys = missingKeys.withColumn("CDC_LOAD_CODE", lit("D")) \
                    .withColumn("LOAD_TS", to_timestamp(lit(pTimeStamp), 'yyyy-MM-dd HH:mm:ss')) \
                    .withColumn("END_TS", lit(self.aAsset["BusinessDate"]))

                # Update T Table for missing keys (soft delete)
                dfTarget = dfTarget.subtract(missingKeys)  # Remove missing keys
                dfTarget = dfTarget.union(missingKeys)  # Add missing keys as deleted

            # Finally, add the new data to the T Table (Insert)
            dfInserts = dfIncoming.withColumn("CDC_LOAD_CODE", lit("I")) \
                .withColumn("LOAD_TS", to_timestamp(lit(pTimeStamp), 'yyyy-MM-dd HH:mm:ss'))

            # Add the new data to T Table
            dfTarget.printSchema()
            dfInserts.printSchema()
            dfTarget = dfTarget.union(dfInserts)

            # Write the final DataFrame back to the Processed Table (T Table)
            dfTarget.write.mode("append").saveAsTable(mainTable)

            self.aLogger.info("applyTempRecordsToTTable.End")
        except Exception as ex:
            self.aLogger.error("applyTempRecordsToTTable.Error: " + str(ex))
            raise Exception(str(ex))

    # def appendAndTrackChangesWithoutMerge(self, dfIncoming, pTimeStamp):
    #     try:
    #         self.aLogger.info("appendAndTrackChangesWithoutMerge.Start")
    #         mainTable = self.aAsset[f'{AssetColumns.Processed_Table_Name}']
    #         businessDate = self.aAsset["BusinessDate"]

    #         self.deleteSegmentFromProcessedTable(dfIncoming)
    #         dfExisting = self.aSpark.table(mainTable).filter("CDC_LOAD_CODE != 'D'")

    #         if self.aInstructions["CTable"]:
    #             # BEFORE IMAGE - changed keys with different checksum
    #             dfBefore = dfIncoming.alias("source").join( \
    #                                 dfExisting.alias("target"), on="KEY_CHECKSUM_TXT", how="inner" \
    #                                 ).filter("target.NON_KEY_CHECKSUM_TXT != source.NON_KEY_CHECKSUM_TXT")
    #             if not dfBefore.rdd.isEmpty():
    #                 cTable = mainTable.replace(".T_", ".C_")
    #                 dfBefore = dfBefore.select("target.*")
    #                 dfBefore = dfBefore.withColumn("CDC_LOAD_CODE", lit("X"))
    #                 dfBefore = dfBefore.withColumn("LOAD_TS", to_timestamp(lit(pTimeStamp), 'yyyy-MM-dd HH:mm:ss'))
    #                 dfBefore.write.mode("append").saveAsTable(cTable)

    #         # CDC Flags
    #         dfUpdates = dfIncoming.alias("new").join(
    #             dfExisting.alias("old"), on="KEY_CHECKSUM_TXT", how="inner"
    #         ).filter("new.NON_KEY_CHECKSUM_TXT != old.NON_KEY_CHECKSUM_TXT")

    #         dfInserts = dfIncoming.join(dfExisting, on="KEY_CHECKSUM_TXT", how="left_anti")

    #         dfDeletes = self.aSpark.createDataFrame([], dfExisting.schema)
    #         if self.aInstructions["Delete"] == "MissingKeys":
    #             dfDeletes = dfExisting.join(dfIncoming, on="KEY_CHECKSUM_TXT", how="left_anti")

    #         dfUpdates = dfUpdates.withColumn("CDC_LOAD_CODE", lit("U"))
    #         dfInserts = dfInserts.withColumn("CDC_LOAD_CODE", lit("I"))
    #         dfDeletes = dfDeletes.withColumn("CDC_LOAD_CODE", lit("D"))

    #         common_columns = list(set(dfInserts.columns) & set(dfUpdates.columns))
    #         # Use selectExpr for alignment
    #         dfInserts = dfInserts.select(*common_columns)
    #         dfUpdates = dfUpdates.select(*common_columns)

    #         dfFinal = dfInserts.union(dfUpdates)
    #         if self.aInstructions["Delete"] == "MissingKeys":
    #             dfFinal = dfFinal.union(dfDeletes)

    #         if self.aInstructions["FromToColumns"]:
    #             dfAffectedKeys = dfFinal.filter("CDC_LOAD_CODE IN ('U','D')").select("KEY_CHECKSUM_TXT").distinct()
    #             dfToClose = dfExisting.join(dfAffectedKeys, on="KEY_CHECKSUM_TXT").filter("END_TS = '2999-12-31'")
    #             dfToClose = dfToClose.withColumn("END_TS", lit(businessDate)).withColumn("CDC_LOAD_CODE", lit("U"))
    #             dfFinal = dfFinal.unionByName(dfToClose)

    #         dfFinal.write.mode("append").saveAsTable(mainTable)

    #         if self.aInstructions["CTable"]:
    #             cTable = mainTable.replace(".T_", ".C_")
    #             dfFinal.write.mode("append").saveAsTable(cTable)

    #         self.aLogger.info("appendAndTrackChangesWithoutMerge.End")
    #     except Exception as ex:
    #         self.aLogger.error("appendAndTrackChangesWithoutMerge.Error: " + str(ex))
    #         raise Exception(ex)

    def appendAndTrackChangesWithoutMerge(self, dfIncoming, pTimeStamp):
        try:
            self.aLogger.info("appendAndTrackChangesWithoutMerge.Start")
            mainTable = self.aAsset[f'{AssetColumns.Processed_Table_Name}']
            businessDate = self.aAsset["BusinessDate"]

            # Delete any segment from the processed table if needed
            self.deleteSegmentFromProcessedTable(dfIncoming)
            dfExisting = self.aSpark.table(mainTable).filter("CDC_LOAD_CODE != 'D'")

            if self.aInstructions["CTable"]:

                # BEFORE IMAGE - changed keys with different checksum
                dfBefore = dfIncoming.alias("source").join(
                    dfExisting.alias("target"), on=col("source.KEY_CHECKSUM_TXT") == col("target.KEY_CHECKSUM_TXT"),
                    how="inner"
                ).filter(col("source.NON_KEY_CHECKSUM_TXT") != col("target.NON_KEY_CHECKSUM_TXT"))

                if not dfBefore.rdd.isEmpty():
                    cTable = mainTable.replace(".T_", ".C_")
                    dfBefore = dfBefore.select("target.*") \
                        .withColumn("CDC_LOAD_CODE", lit("X")) \
                        .withColumn("LOAD_TS", to_timestamp(lit(pTimeStamp), 'yyyy-MM-dd HH:mm:ss'))
                    dfBefore.write.mode("append").saveAsTable(cTable)

            # CDC Flags
            dfUpdates = dfIncoming.alias("new").join(
                dfExisting.alias("old"), on=col("new.KEY_CHECKSUM_TXT") == col("old.KEY_CHECKSUM_TXT"), how="inner"
            ).filter(col("new.NON_KEY_CHECKSUM_TXT") != col("old.NON_KEY_CHECKSUM_TXT"))

            dfInserts = dfIncoming.join(
                dfExisting, on=col("dfIncoming.KEY_CHECKSUM_TXT") == col("dfExisting.KEY_CHECKSUM_TXT"), how="left_anti"
            )

            # Delete Logic for Missing Keys
            dfDeletes = self.aSpark.createDataFrame([], dfExisting.schema)
            if self.aInstructions["Delete"] == "MissingKeys":
                dfDeletes = dfExisting.join(dfIncoming,
                                            on=col("dfExisting.KEY_CHECKSUM_TXT") == col("dfIncoming.KEY_CHECKSUM_TXT"),
                                            how="left_anti")

            # Add CDC load codes
            dfUpdates = dfUpdates.withColumn("CDC_LOAD_CODE", lit("U"))
            dfInserts = dfInserts.withColumn("CDC_LOAD_CODE", lit("I"))
            dfDeletes = dfDeletes.withColumn("CDC_LOAD_CODE", lit("D"))

            dfFinal = dfInserts.unionByName(dfUpdates)
            if self.aInstructions["Delete"] == "MissingKeys":
                dfFinal = dfFinal.unionByName(dfDeletes)

            if self.aInstructions["FromToColumns"]:
                dfAffectedKeys = dfFinal.filter(col("CDC_LOAD_CODE").isin("U", "D")).select(
                    "KEY_CHECKSUM_TXT").distinct()
                dfToClose = dfExisting.join(dfAffectedKeys, on=col("dfExisting.KEY_CHECKSUM_TXT") == col(
                    "dfAffectedKeys.KEY_CHECKSUM_TXT")) \
                    .filter(col("dfExisting.END_TS") == "2999-12-31")
                dfToClose = dfToClose.withColumn("END_TS", lit(businessDate)).withColumn("CDC_LOAD_CODE", lit("U"))
                dfFinal = dfFinal.unionByName(dfToClose)

            # Write final data to main table
            dfFinal.write.mode("append").saveAsTable(mainTable)

            # If there is a C table, also write to it
            if self.aInstructions["CTable"]:
                cTable = mainTable.replace(".T_", ".C_")
                dfFinal.write.mode("append").saveAsTable(cTable)

            self.aLogger.info("appendAndTrackChangesWithoutMerge.End")
        except Exception as ex:
            self.aLogger.error("appendAndTrackChangesWithoutMerge.Error: " + str(ex))
            raise Exception(ex)

    def mergeAndTrackChangesWithDelta(self, dfIncoming, pTimeStamp):
        try:
            pTimeStamp_utc = to_timestamp(lit(pTimeStamp))
            pTimeStamp_est = date_format(pTimeStamp_utc.cast(TimestampType()) + expr("INTERVAL 5 HOURS"),
                                         "yyyy-MM-dd HH:mm:ss")

            if self.aInstructions["Merge"]:
                self.aLogger.info("mergeAndTrackChangesWithDelta.Start")

                dfSource = dfIncoming

                self.saveBeforeImageToCTable(dfSource, pTimeStamp_est)

                deltaTable = DeltaTable.forName(self.aSpark, self.aAsset[f'{AssetColumns.Processed_Table_Name}'])

                dUpdateColumns = {col: f"dfSource.{col}" for col in dfSource.columns if col != 'KEY_CHECKSUM_TXT'}
                dUpdateColumns['CDC_LOAD_CODE'] = "'U'"

                dInsertColumns = {col: f"dfSource.{col}" for col in dfSource.columns}
                dInsertColumns['CDC_LOAD_CODE'] = "'I'"

                dUpdateColumnsSourceDeleted = {col: f"dfSource.{col}" for col in dfSource.columns if
                                               col != 'KEY_CHECKSUM_TXT'}
                dUpdateColumnsSourceDeleted['CDC_LOAD_CODE'] = "'D'"

                if self.aInstructions["Delete"] == "None":
                    self.aLogger.info("mergeAndTrackChangesWithDelta.Merge.WithoutSoftDelete")
                    deltaTable.alias("dfTarget").merge(
                        dfSource.alias("dfSource"),
                        "dfTarget.KEY_CHECKSUM_TXT = dfSource.KEY_CHECKSUM_TXT"
                    ).whenMatchedUpdate(
                        condition="dfTarget.NON_KEY_CHECKSUM_TXT != dfSource.NON_KEY_CHECKSUM_TXT",
                        set=dUpdateColumns
                    ).whenNotMatchedInsert(
                        values=dInsertColumns
                    ).execute()

                elif self.aInstructions["Delete"] == "DeleteFlag":
                    self.aLogger.info("mergeAndTrackChangesWithDelta.Merge.WithSourceDeleteFlag")
                    for dField in self.aAsset["Fields"]:
                        if dField[f'{FieldColumns.Action_Flags_String}'] != '':
                            sDeleteFlag = dField[f'{FieldColumns.Target_Field_Name}'] + ' = "{}"'.format(
                                dField[f'{FieldColumns.Action_Flags_String}'])
                            sNegateDeleteFlag = dField[f'{FieldColumns.Target_Field_Name}'] + ' != "{}"'.format(
                                dField[f'{FieldColumns.Action_Flags_String}'])
                            break
                    deltaTable.alias("dfTarget").merge(
                        dfSource.alias("dfSource"),
                        "dfTarget.KEY_CHECKSUM_TXT = dfSource.KEY_CHECKSUM_TXT and dfTarget.CDC_LOAD_CODE != 'D'"
                    ).whenMatchedUpdate(
                        condition=f"dfSource.{sNegateDeleteFlag} and dfTarget.NON_KEY_CHECKSUM_TXT != dfSource.NON_KEY_CHECKSUM_TXT",
                        set=dUpdateColumns
                    ).whenMatchedUpdate(
                        condition=f"dfSource.{sDeleteFlag}",
                        set=dUpdateColumnsSourceDeleted
                    ).whenNotMatchedInsert(
                        values=dInsertColumns
                    ).execute()

                elif self.aInstructions["Delete"] == "MissingKeys":
                    self.aLogger.info("mergeAndTrackChangesWithDelta.Merge.WithSoftDelete")
                    deltaTable.alias("dfTarget").merge(
                        dfSource.alias("dfSource"),
                        "dfTarget.KEY_CHECKSUM_TXT = dfSource.KEY_CHECKSUM_TXT and dfTarget.CDC_LOAD_CODE != 'D'"
                    ).whenMatchedUpdate(
                        condition="dfTarget.NON_KEY_CHECKSUM_TXT != dfSource.NON_KEY_CHECKSUM_TXT",
                        set=dUpdateColumns
                    ).whenNotMatchedInsert(
                        values=dInsertColumns
                    ).whenNotMatchedBySourceUpdate(
                        condition="dfTarget.CDC_LOAD_CODE != 'D'",
                        set={
                            "CDC_LOAD_CODE": lit("D"),
                            "LOAD_TS": to_timestamp(lit(pTimeStamp_est), 'yyyy-MM-dd HH:mm:ss')
                        }
                    ).execute()

                elif self.aInstructions["Delete"] == "Soft.Segment":
                    self.aLogger.info("mergeAndTrackChangesWithDelta.Merge.WithSegmentDelete")
                    sSnapshotColumn = None
                    for dFields in self.aAsset["Fields"]:
                        if dFields[f'{FieldColumns.Snapshot_Indicator}'] == "Y":
                            sSnapshotColumn = dFields[f'{FieldColumns.Target_Field_Name}']
                            break
                    dfSourceDistinctSnapshot = dfSource.select(sSnapshotColumn).distinct()
                    lSnapshotValues = [row[0] for row in dfSourceDistinctSnapshot.collect()]
                    sFormattedSnapshots = ",".join([f"'{item}'" for item in lSnapshotValues])

                    sMergeCondition = f"dfTarget.KEY_CHECKSUM_TXT = dfSource.KEY_CHECKSUM_TXT and dfTarget.CDC_LOAD_CODE != 'D'"
                    sMergeConditionUpdate = f"dfTarget.{sSnapshotColumn} IN ({sFormattedSnapshots}) AND dfTarget.NON_KEY_CHECKSUM_TXT != dfSource.NON_KEY_CHECKSUM_TXT"
                    sMergeConditionDelete = f"dfTarget.{sSnapshotColumn} IN ({sFormattedSnapshots}) and dfTarget.CDC_LOAD_CODE != 'D'"

                    deltaTable.alias("dfTarget").merge(
                        dfSource.alias("dfSource"),
                        sMergeCondition
                    ).whenMatchedUpdate(
                        condition=sMergeConditionUpdate,
                        set=dUpdateColumns
                    ).whenNotMatchedInsert(
                        values=dInsertColumns
                    ).whenNotMatchedBySourceUpdate(
                        condition=sMergeConditionDelete,
                        set={
                            "CDC_LOAD_CODE": lit("D"),
                            "LOAD_TS": to_timestamp(lit(pTimeStamp_est), 'yyyy-MM-dd HH:mm:ss')
                        }
                    ).execute()

                self.saveAfterImageToCTable(deltaTable, pTimeStamp_est)

                self.aLogger.info("mergeAndTrackChangesWithDelta.End")

        except Exception as ex:
            self.aLogger.error("mergeAndTrackChangesWithDelta.Error: " + str(ex))
            raise

    def saveBeforeImageToCTable(self, dfSource, pTimeStamp_est):
        try:
            if not self.aInstructions["CTable"]:
                return

            sCtableName = self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_")
            dfTargetBefore = self.aSpark.table(self.aAsset[f'{AssetColumns.Processed_Table_Name}'])

            if self.aInstructions["Delete"] == "DeleteFlag":
                for dField in self.aAsset["Fields"]:
                    if dField['Action_Flags_String'] != '':
                        dfSource = dfSource.filter(col(dField[f'{FieldColumns.Target_Field_Name}']) != dField[
                            f'{FieldColumns.Action_Flags_String}'])
                        break

            dfBeforeImage = dfTargetBefore.alias("target").join(
                dfSource.alias("source"),
                on=[
                    col("target.KEY_CHECKSUM_TXT") == col("source.KEY_CHECKSUM_TXT"),
                    col("target.NON_KEY_CHECKSUM_TXT") != col("source.NON_KEY_CHECKSUM_TXT")
                ],
                how="inner"
            ).select("target.*")

            windowSpec = Window.partitionBy('KEY_CHECKSUM_TXT').orderBy(col('LOAD_TS').desc())
            dfBeforeImage = dfBeforeImage.withColumn('row_number', row_number().over(windowSpec))
            dfBeforeImage = dfBeforeImage.filter(col('row_number') == 1).drop("row_number")
            dfBeforeImage = dfBeforeImage.filter(col('CDC_LOAD_CODE').isin('I', 'U'))
            dfBeforeImage = dfBeforeImage.withColumn("CDC_LOAD_CODE", lit("X"))
            dfBeforeImage = dfBeforeImage.withColumn("LOAD_TS",
                                                     to_timestamp(lit(pTimeStamp_est), 'yyyy-MM-dd HH:mm:ss'))

            required_columns = self.aSpark.table(sCtableName).columns
            dfBeforeImage = dfBeforeImage.select(*required_columns)
            dfBeforeImage.write.mode("append").saveAsTable(sCtableName)
        except Exception as ex:
            self.aLogger.error("saveBeforeImageToCTable.Error: " + str(ex))
            raise

    def saveAfterImageToCTable(self, deltaTable, pTimeStamp_est):
        try:
            if not self.aInstructions["CTable"]:
                return

            sCtableName = self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_")
            required_columns = self.aSpark.table(sCtableName).columns

            dfAfterImage = deltaTable.toDF().filter(
                (col("CDC_LOAD_CODE").isin(["I", "U", "D"])) &
                (col("LOAD_TS") == to_timestamp(lit(pTimeStamp_est), 'yyyy-MM-dd HH:mm:ss'))
            )
            dfAfterImage = dfAfterImage.select(*required_columns)
            dfAfterImage.write.mode("append").saveAsTable(sCtableName)
        except Exception as ex:
            self.aLogger.error("saveAfterImageToCTable.Error: " + str(ex))
            raise

    def truncateTable(self, pTableName):
        try:
            self.aLogger.info("truncateTable.Start: " + pTableName)
            sSQL = f"DELETE FROM {pTableName}"
            self.executeSQL(sSQL)
            self.aLogger.info("truncateTable.End")
        except Exception as ex:
            self.aLogger.error("truncateTable.Error: " + str(ex))
            raise Exception(ex)

    def getRawDataFrame(self, pTimeStamp):
        try:
            self.aLogger.info("getRawDataFrame.Start")
            self.aSpark.sql("REFRESH TABLE " + self.aAsset[AssetColumns.Raw_Table_Name])

            sSQL = """
                SELECT *, 'I' AS CDC_LOAD_CODE,
                       CAST('<TimeStamp> EST' AS TIMESTAMP) AS LOAD_TS
                FROM <RawTable>
                WHERE DIFSourceFile = '<SourceFile>'
                  AND BusinessDate = '<BusinessDate>'
                """
            sSQL = sSQL.replace("<TimeStamp>", pTimeStamp)
            sSQL = sSQL.replace("<RawTable>", self.aAsset[f'{AssetColumns.Raw_Table_Name}'])
            sSQL = sSQL.replace("<SourceFile>", self.aAsset["IngestFile"])
            sSQL = sSQL.replace("<BusinessDate>", self.aAsset["BusinessDate"])

            self.aLogger.debug("RawDF SQL: " + sSQL)
            df = self.aSpark.sql(sSQL)
            self.aLogger.debug("This is the data from the Raw table to be processed")
            df.show()
            self.aLogger.info("getRawDataFrame.End")
            return df
        except Exception as ex:
            self.aLogger.error("getRawDataFrame.Error: " + str(ex))
            raise Exception(ex)

    def createProcessedTable(self, pTableName):
        try:
            self.aLogger.info("createProcessedTable.Start: " + pTableName)
            sColumnList = self.getColumns()

            if self.aInstructions["HashColumns"]:
                sColumnList += ", KEY_CHECKSUM_TXT STRING, NON_KEY_CHECKSUM_TXT STRING"
            if self.aInstructions["VersionColumn"]:
                sColumnList += ", VERSION_ID INT"
            if self.aInstructions["FromToColumns"]:
                sColumnList += ", START_TS STRING, END_TS STRING"

            sColumnList += ", BusinessDate DATE, DIFSourceFile STRING, DIFLoadDate TIMESTAMP, LOAD_TS TIMESTAMP, CDC_LOAD_CODE STRING"
            for dField in self.aAsset["Fields"]:
                if 'Encrypt' in dField.get(f'{FieldColumns.Data_Protection_Code}', ''):
                    sColumnList += ", DP_Encrypter_Passphrase_Version STRING"
                    break

            sSQL = f"CREATE TABLE IF NOT EXISTS {pTableName} ({sColumnList})"

            lPartitionFields = [
                dField[f'{FieldColumns.Target_Field_Name}']
                for dField in self.aAsset["Fields"]
                if dField.get("Partitioned_Indicator") == "Y"
            ]

            if lPartitionFields:
                self.aLogger.info("lPartitionFields: " + str(lPartitionFields))
                sSQL += " PARTITIONED BY (" + ", ".join(lPartitionFields) + ")"

            self.executeSQL(sSQL)
            self.aLogger.info("createProcessedTable.End")
        except Exception as ex:
            self.aLogger.error("createProcessedTable.Error: " + str(ex))
            raise

    def deleteSegmentFromProcessedTable(self, dfIncoming):
        try:
            self.aLogger.info("deleteSegmentFromProcessedTable.Start")
            if self.aInstructions["Delete"] != "Hard.Segment":
                self.aLogger.info("deleteSegmentFromProcessedTable.Skipped")
                return

            snapshotField = self.getSnapshotField()
            snapshotValues = dfIncoming.select(snapshotField).distinct().rdd.flatMap(lambda x: x).collect()
            if not snapshotValues:
                self.aLogger.info("No snapshot values found for Hard.Segment delete")
                return

            formattedValues = ",".join(f"'{val}'" for val in snapshotValues)
            tableName = self.aAsset[f'{AssetColumns.Processed_Table_Name}']
            sSQL = f"DELETE FROM {tableName} WHERE {snapshotField} IN ({formattedValues})"
            self.executeSQL(sSQL)

            self.aLogger.info("deleteSegmentFromProcessedTable.End")
        except Exception as ex:
            self.aLogger.error("deleteSegmentFromProcessedTable.Error: " + str(ex))
            raise Exception(ex)

    def getColumns(self):
        try:
            self.aLogger.debug("getColumns.Start")
            sColumnList = ""
            sorted_fields = sorted(self.aAsset["Fields"], key=lambda x: x.get("Field_Sequence_Number", 0))

            for dField in sorted_fields:
                if dField.get(f'{FieldColumns.Exclude_From_Processed_Indicator}') == "Y":
                    continue
                if sColumnList:
                    sColumnList += ", "
                sColumnList += dField[f'{FieldColumns.Target_Field_Name}'] + " " + dField[
                    f'{FieldColumns.Target_Data_Type_Code}']

            self.aLogger.debug("getColumns.End")
            return sColumnList
        except Exception as ex:
            self.aLogger.error("getColumns.Error: " + str(ex))
            raise

    def getDecryptedSelectClause(self):
        try:
            self.aLogger.debug("getDecryptedSelectClause.Start")
            lSelectColumns = []

            for dField in self.aAsset["Fields"]:
                sFieldName = dField[f'{FieldColumns.Target_Field_Name}']
                if 'Encrypt' in dField.get(f'{FieldColumns.Data_Protection_Code}', ''):
                    lSelectColumns.append(
                        f"db_security.f_dp_decrypt({sFieldName}, DP_Encrypter_Passphrase_Version) AS {sFieldName}")
                else:
                    lSelectColumns.append(sFieldName)

            lSelectColumns += ["BusinessDate", "DIFSourceFile", "DIFLoadDate", "LOAD_TS", "CDC_LOAD_CODE"]
            sSelectClause = ", ".join(lSelectColumns)
            self.aLogger.debug("getDecryptedSelectClause.End")
            return sSelectClause

        except Exception as ex:
            self.aLogger.error("getDecryptedSelectClause.Error: " + str(ex))
            raise

    def shouldCreateDecryptionView(self):
        try:
            self.aLogger.debug("shouldCreateDecryptionView.Start")
            for dField in self.aAsset["Fields"]:
                if 'Encrypt' in dField.get(f'{FieldColumns.Data_Protection_Code}', ''):
                    return True
            return False
        except Exception as ex:
            self.aLogger.error("shouldCreateDecryptionView.Error: " + str(ex))
            raise