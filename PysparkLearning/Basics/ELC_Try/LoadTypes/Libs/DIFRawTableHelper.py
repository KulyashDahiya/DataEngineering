# Handles all RAW table creation etc.

from DatabaseConstants import TableNames,ProjectColumns,AssetGroupColumns,AssetColumns,FieldColumns,IncrementalLoadColumns,AssetHistoryColumns,AssetStatusColumns,HistoricalLoadColumns,HolidayCalendarColumns

class DIFRawTableHelper:
    def __init__(self,pEnvConfig,pAssetGroupConfig,pLogger,pSpark,pAsset):
        self.aEnvConfig = pEnvConfig
        self.aAssetGroupConfig = pAssetGroupConfig
        self.aLogger = pLogger
        self.aSpark = pSpark
        self.aAsset = pAsset

    def dropRawTable(self):
        try:
            self.aLogger.info("DIFTableHelper.dropRawTable.Start.250225")
            G_SQL = "DROP TABLE IF EXISTS <TableName>;"
            sSQL = G_SQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Raw_Table_Name}'])
            self.executeSQL(sSQL)
            self.aLogger.info(sSQL)
            self.aLogger.info("DIFTableHelper.dropRawTable.End")
            return
        except Exception as ex:
            self.aLogger.error("DIFTableHelper.dropRawTable.Error:" + str(ex))
            raise Exception(ex)

            # If this is not needed - lets remove the code
    def dropRawPartition(self):
        try:
            self.aLogger.info("DIFTableHelper.dropRawPartition.Start")
            # G_SQL = "ALTER TABLE <TableName> DROP IF EXISTS PARTITION (BusinessDate='<BusinessDate>', DIFSourceFile='<SourceFile>');"
            G_SQL = "DELETE FROM <TableName> WHERE BusinessDate='<BusinessDate>' AND DIFSourceFile='<SourceFile>';" 
            sSQL = G_SQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Raw_Table_Name}'])
            # sSQL = sSQL.replace("<SourceFile>", self.aAsset["IngestFile"])
            sSQL = sSQL.replace("<SourceFile>", self.aAsset["ShortFile"])
            sSQL = sSQL.replace("<BusinessDate>", self.aAsset["BusinessDate"])
            # self.executeSQL(sSQL)
            self.aLogger.info("DIFTableHelper.dropRawPartition.End")

        except Exception as ex:
            self.aLogger.info("DIFTableHelper.dropRawPartition.Error:"+str(ex))
            raise Exception(ex)

    

    def createRawTable(self):
        try:
            self.aLogger.info("createRawTableParquet.Start")
            self.aLogger.debug("Raw Folder:" + self.aAsset["RawFolder"])
            G_SQL = 'Create table IF NOT EXISTS <TableName> (<ColumnList>, \
                        BusinessDate Date, DIFSourceFile String, DIFLoadDate TIMESTAMP) \
                        Using PARQUET Partitioned By (BusinessDate,DIFSourceFile) OPTIONS (path "<FolderRaw>/");'
            for dField in self.aAsset["Fields"]:
                if 'Encrypt' in dField[f'{FieldColumns.Data_Protection_Code}']:
                    G_SQL = 'Create table IF NOT EXISTS <TableName> (<ColumnList>, \
                        DP_Encrypter_Passphrase_Version String,BusinessDate Date, DIFSourceFile String, DIFLoadDate TIMESTAMP) \
                        Using PARQUET Partitioned By (BusinessDate,DIFSourceFile) OPTIONS (path "<FolderRaw>");'
                    break
            sSQL = G_SQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Raw_Table_Name}'])
            sSQL = sSQL.replace("<FolderRaw>", self.aAsset["RawFolder"])
            sColumnList = self.getColumns()
            sSQL = sSQL.replace("<ColumnList>", sColumnList)
            self.executeSQL(sSQL)
            self.aLogger.info("createRawTableParquet.End")
        except Exception as ex:
            self.aLogger.error("createRawTableParquet.Error:" + str(ex))

    # If this is not needed - lets remove
    def addRawPartition(self):
        try:
            self.aLogger.info("DIFTableHelper.addRawPartitionParquet.Start")
            # sRawFile = self.aAsset["RawFolderWithDate"] + "/" + self.aAsset["ShortFile"]
            # self.aLogger.info(sRawFile)
            # G_SQL = "ALTER TABLE <TableName>  ADD PARTITION (BusinessDate='<BusinessDate>', DIFSourceFile='<SourceFile>') LOCATION '<RawFile>'; "
            # sSQL = G_SQL.replace("<TableName>", self.aAsset[f'{AssetColumns.Raw_Table_Name}'])
            # sSQL = sSQL.replace("<RawFile>", sRawFile)
            # sSQL = sSQL.replace("<SourceFile>", self.aAsset["IngestFile"])
            # sSQL = sSQL.replace("<BusinessDate>", self.aAsset["BusinessDate"])
            self.aLogger.info("Unity Catalog does NOT support ADD PARTITION, auto-tracks partitions")
            # self.executeSQL(sSQL) 
            self.aLogger.info("Skipping addRawPartition - Not required for Unity Catalog") 
            self.aLogger.info("DIFTableHelper.addRawPartitionParquet.End")

        except Exception as ex:
            self.aLogger.info("DIFTableHelper.addRawPartitionParquet.Error:"+str(ex))
            raise Exception(ex)
    # HELPERS
    def executeSQL(self, pSQL):
        self.aLogger.debug(pSQL)
        self.aSpark.sql(pSQL)

    def getColumns(self):
        sColumnList = ""
        
        # Sorted fields based on Field_Sequence_Number
        sorted_fields = sorted(self.aAsset["Fields"], key=lambda x: x["Field_Sequence_Number"])
        
        for dField in sorted_fields:
            if sColumnList:
                sColumnList += ", "
            
            sColumnList = sColumnList + dField[f'{FieldColumns.Target_Field_Name}'] + " " + dField[f'{FieldColumns.Source_Data_Type_Code}']
        
        return sColumnList
