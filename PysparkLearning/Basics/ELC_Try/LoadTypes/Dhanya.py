from pyspark.sql.functions import concat, lit, upper, lower, to_timestamp, expr
from pyspark.sql.functions import concat_ws, md5, coalesce
from pyspark.sql.functions import lit, to_timestamp, col, date_format, row_number
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

from DatabaseConstants import TableNames, ProjectColumns, AssetGroupColumns, AssetColumns, FieldColumns, \
    IncrementalLoadColumns, AssetHistoryColumns, AssetStatusColumns, HistoricalLoadColumns, HolidayCalendarColumns

# This holds all the flags for each Load Type
G_TempSuffix = "_temp"

G_LoadTypeInstructions = {
    "T4-All-None": {
        "Description": "Get full data; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "None", "LatestView": False, "Merge": True},
    "T4-All-SenseDelete": {
        "Description": "Get all data, missing key means delete; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "MissingKeys", "LatestView": False, "Merge": True},
    "T4-Subset-SourceDeleteFlag": {
        "Description": "Get all data, missing key means delete; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "DeleteFlag", "LatestView": False, "Merge": True},
    "T4-Subset-None": {
        "Description": "Get subset of data; mark changes",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "None", "LatestView": False, "Merge": True},
    "T4-Segment-Delete": {
        "Description": "Get subset of data; mark changes: NOT YET SUPPORTED",
        "HashColumns": True, "VersionColumn": False, "FromToColumns": False,
        "CTable": True, "Delete": "Soft.Segment", "LatestView": False, "Merge": True}
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
            self.aLogger.debug("createCTables.Start")
            self.createProcessedTable(self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_"))
        self.aLogger.debug("createProcessedTables.End")

    def updateLatestView(self):
        sSQL = "CREATE OR REPLACE VIEW <V_View> AS SELECT <ColumnList>,Load_TS,CDC_LOAD_CODE FROM <TTable> <Where>"
        sWhere = "WHERE 1=1"
        if self.aInstructions["FromToColumns"]:
            sWhere = sWhere + " and END_TS='2999-12-31'"
        sSQL = sSQL.replace("<Where>", sWhere)
        sSQL = sSQL.replace("<V_View>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".V_"))
        sSQL = sSQL.replace("<TTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
        sColumnList = self.getColumns()
        sColumnList = ",".join([col.split()[0] for col in sColumnList.split(",")])
        sSQL = sSQL.replace("<ColumnList>", sColumnList)
        self.executeSQL(sSQL)
        self.aLogger.info("updateLatestView.End")
        return

    def createProcessedTable(self, pTableName):
        self.aLogger.debug("createProcessedTable.Start:" + pTableName)
        sSQL = "Create table IF NOT EXISTS <TableName> (<ColumnList> "
        # always add these

        # Either VersionID or other stuff
        if self.aInstructions["HashColumns"]:
            sSQL = sSQL + ",KEY_CHECKSUM_TXT String, NON_KEY_CHECKSUM_TXT String "
        if self.aInstructions["VersionColumn"]:
            sSQL = sSQL + ", VERSION_ID int "
        if self.aInstructions["FromToColumns"]:
            sSQL = sSQL + ", START_TS String, END_TS String "

        # ALWAYS ADDED
        sSQlfinal = sSQL + " ,BusinessDate Date, DIFSourceFile String, DIFLoadDate TIMESTAMP, LOAD_TS TIMESTAMP, CDC_LOAD_CODE STRING"
        for dField in self.aAsset["Fields"]:
            if 'Encrypt' in dField[f'{FieldColumns.Data_Protection_Code}']:
                sSQlfinal = sSQL + " ,DP_Encrypter_Passphrase_Version String, BusinessDate Date, DIFSourceFile String, DIFLoadDate TIMESTAMP, LOAD_TS TIMESTAMP, CDC_LOAD_CODE STRING"
                break

        sSQlfinal = sSQlfinal + ")"
        sSQlfinal = sSQlfinal.replace("<TableName>", pTableName)
        sColumnList = self.getColumns()
        sSQlfinal = sSQlfinal.replace("<ColumnList>", sColumnList)
        self.executeSQL(sSQlfinal)
        self.aLogger.debug("createProcessedTable.End")

    # This needs to build an Initial temp table

    def applyRecordsToDeltaTableAndCTable(self, pTimeStamp):
        try:
            pTimeStamp_utc = to_timestamp(lit(pTimeStamp))
            pTimeStamp_est = date_format(pTimeStamp_utc.cast(TimestampType()) + expr("INTERVAL 5 HOURS"),
                                         "yyyy-MM-dd HH:mm:ss")

            if self.aInstructions["Merge"]:
                self.aLogger.info("applyRecordsToDeltaTableAndCTable.Start")

                # Raw → Adjusted
                dfRaw = self.getRawDataFrame(pTimeStamp)
                dfSource = self.getAdjustedRawDataFrame(dfRaw)

                # -------------------- BEFORE IMAGE (C Table Pre-Merge) --------------------
                if self.aInstructions["CTable"]:
                    sCtableName = self.aAsset[f'{AssetColumns.Processed_Table_Name}'].replace(".T_", ".C_")
                    dfTargetBefore = self.aSpark.table(self.aAsset[f'{AssetColumns.Processed_Table_Name}'])

                    if self.aInstructions["Delete"] == "DeleteFlag":
                        for dField in self.aAsset["Fields"]:
                            if dField['Action_Flags_String'] != '':
                                dfSource = dfSource.filter(
                                    col(dField[f'{FieldColumns.Target_Field_Name}']) != dField[
                                        f'{FieldColumns.Action_Flags_String}']
                                )
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

                # -------------------- MERGE LOGIC --------------------
                deltaTable = DeltaTable.forName(self.aSpark, self.aAsset[f'{AssetColumns.Processed_Table_Name}'])

                dUpdateColumns = {col: f"dfSource.{col}" for col in dfSource.columns if col != 'KEY_CHECKSUM_TXT'}
                dUpdateColumns['CDC_LOAD_CODE'] = "'U'"

                dInsertColumns = {col: f"dfSource.{col}" for col in dfSource.columns}
                dInsertColumns['CDC_LOAD_CODE'] = "'I'"

                dUpdateColumnsSourceDeleted = {col: f"dfSource.{col}" for col in dfSource.columns if
                                               col != 'KEY_CHECKSUM_TXT'}
                dUpdateColumnsSourceDeleted['CDC_LOAD_CODE'] = "'D'"

                if self.aInstructions["Delete"] == "None":
                    self.aLogger.info("applyRecordsToDeltaTableAndCTable.Merge.WithoutSoftDelete")
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
                    self.aLogger.info("applyRecordsToDeltaTableAndCTable.Merge.WithSourceDeleteFlag")
                    for dField in self.aAsset["Fields"]:
                        if dField[f'{FieldColumns.Action_Flags_String}'] != '':
                            sDeleteFlag = dField[f'{FieldColumns.Target_Field_Name}'] + ' = \'{}\''.format(
                                dField[f'{FieldColumns.Action_Flags_String}'])
                            sNegateDeleteFlag = dField[f'{FieldColumns.Target_Field_Name}'] + ' != \'{}\''.format(
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
                    self.aLogger.info("applyRecordsToDeltaTableAndCTable.Merge.WithSoftDelete")
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
                    self.aLogger.info("applyRecordsToDeltaTableAndCTable.Merge.WithSegmentDelete")
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

            # -------------------- AFTER IMAGE (C Table Post-Merge) --------------------
            if self.aInstructions["CTable"]:
                dfAfterImage = deltaTable.toDF().filter(
                    (col("CDC_LOAD_CODE").isin(["I", "U", "D"])) &
                    (col("LOAD_TS") == to_timestamp(lit(pTimeStamp_est), 'yyyy-MM-dd HH:mm:ss'))
                )
                dfAfterImage = dfAfterImage.select(*required_columns)
                dfAfterImage.write.mode("append").saveAsTable(sCtableName)

            self.aLogger.info("applyRecordsToDeltaTableAndCTable.End")

        except Exception as ex:
            self.aLogger.error("applyRecordsToDeltaTableAndCTable.Error: " + str(ex))
            raise

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
            sSQL = sSQL.replace("<BusinessDate>", self.aAsset[f"{AssetHistoryColumns.Business_Date}"])

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
            df = self.renameColumns(df)
            df = self.addVersionIDColumn(df)
            df = self.addHashColumns(df)
            df = self.addFromToColumns(df)
            # To be removed
            df.display()
            self.aLogger.debug("getAdjustedRawDataFrame.End")
            return df
        except Exception as ex:
            self.aLogger.debug("getAdjustedRawDataFrame.Error:" + str(ex))
            raise Exception(ex)

    def renameColumns(self, pDF):
        '''
        Renames the columns in the dataframe -- Mapping the Source column names with target column names
        Parameters: pDF - Dataframe
        Returns: Dataframe with renamed columns
        '''
        try:
            self.aLogger.debug("renameColumns.Start")
            df = pDF
            for field in self.aAsset['Fields']:
                source_field = field['Source_Field_Name']
                target_field = field['Target_Field_Name']
                # Renaming the columns
                df = df.withColumnRenamed(source_field, target_field)
            self.aLogger.debug("renameColumns.End")
            return df
        except Exception as ex:
            self.aLogger.error("renameColumns.Error:" + str(ex))
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
                    lKeys.append(coalesce(col(oF[f'{FieldColumns.Target_Field_Name}']), lit('NULL')))
                else:
                    lNonKeys.append(coalesce(col(oF[f'{FieldColumns.Target_Field_Name}']), lit('NULL')))
            df = df.withColumn('KEY_CHECKSUM_TXT', md5(concat_ws(',^', *lKeys)))
            df = df.withColumn('NON_KEY_CHECKSUM_TXT', md5(concat_ws(',^', *lNonKeys)))
            self.aLogger.debug("addHashColumns.End")
            return df
        except Exception as ex:
            self.aLogger.error("addHashColumns.Error:" + str(ex))
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
            df = df.withColumn('START_TS', lit(self.aAsset[f"{AssetHistoryColumns.Business_Date}"]))
            df = df.withColumn('END_TS', lit(sLater))
            self.aLogger.debug("addFromToColumns.End")
            return df
        except Exception as ex:
            self.aLogger.error("addFromToColumns.Error:" + str(ex))
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

    def truncateTableSegment(self):
        try:
            self.aLogger.info("truncateTableSegment.Start.25.02.26")
            sSQL = "Delete from <TableName> where <Field> in (Select distinct <Field> from <TempTable>);"
            sSQL = sSQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'])
            sSQL = sSQL.replace("<TempTable>", self.aAsset[f'{AssetColumns.Processed_Table_Name}'] + G_TempSuffix)
            sSQL = sSQL.replace("<Field>", self.getSnapshotField())
            self.executeSQL(sSQL)
            self.aLogger.info("truncateTableSegment.End.OK")
        except Exception as ex:
            self.aLogger.error("truncateTableSegment.Error:" + str(ex))
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
        for dField in self.aAsset["Fields"]:
            if sColumnList != "":
                sColumnList = sColumnList + ","
            sColumnList = sColumnList + dField[f'{FieldColumns.Target_Field_Name}'] + " " + dField[
                f'{FieldColumns.Target_Data_Type_Code}']
        return sColumnList