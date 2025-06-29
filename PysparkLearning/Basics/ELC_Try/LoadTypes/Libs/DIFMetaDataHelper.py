# Desc: All access to DIF MetaData
import os
import datetime
from pyspark.sql import SparkSession
from DatabaseConstants import TableNames,ProjectColumns,AssetGroupColumns,AssetColumns,FieldColumns,IncrementalLoadColumns,AssetHistoryColumns,AssetStatusColumns,HistoricalLoadColumns,HolidayCalendarColumns

G_SQL_GetProject = f"(select * from <DIFSCHEMA>.{TableNames.Project} where {ProjectColumns.ProjectName}='<ProjectName>')a1"
G_SQL_GetAssetGroup = f"(select * from <DIFSCHEMA>.{TableNames.AssetGroup} where {AssetGroupColumns.Project_Key} = <ProjectID> and {AssetGroupColumns.Asset_Group_Name} = '<AssetGroupName>') a1"
G_SQL_GetAssets = f"(select A.*, S.Current_Asset_Status_Code from <DIFSCHEMA>.{TableNames.Asset} AS A LEFT JOIN <DIFSCHEMA>.Asset_Status AS S ON A.Asset_Key = S.Asset_Key where {AssetColumns.Asset_Group_Key} = <AssetGroupID>) a1"
G_SQL_GetFields = f"(select * from <DIFSCHEMA>.{TableNames.Field} where {FieldColumns.Asset_Key}=<AssetID>)a1"
G_SQL_GetIIField = f"(select * from <DIFSCHEMA>.{TableNames.IncrementalIdentifier} where {AssetColumns.Asset_Key}=<AssetID> and {FieldColumns.Source_Field_Name} = '<SourceFieldName>')a1"


class DIFMetaDataHelper:
    def __init__(self, pEnvConfig, pLogger):
        self.aEnvConfig = pEnvConfig
        self.aLogger = pLogger

    def getLastValueOfIIField(self,pAsset,pField):
        try:
            sSQL = G_SQL_GetIIField
            sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
            sSQL = sSQL.replace("<AssetID>",str(pAsset[f'{AssetColumns.Asset_Key}']))
            sSQL = sSQL.replace("<SourceFieldName>", str(pField[f"{FieldColumns.Source_Field_Name}"]))
            spark = SparkSession.builder.appName(pAsset[f"{AssetColumns.Asset_Name}"]).getOrCreate()
            self.aLogger.debug(sSQL)
            dfSpark = spark.read.jdbc(url=self.aEnvConfig["MetaData_URL"],
                                      table=sSQL,
                                      properties=self.aEnvConfig["MetaData_ConnProps"])
            dfPandas = dfSpark.select("*").toPandas()
            dRecord = dfPandas.to_dict('records')[0]
            return dRecord["LastValue"]

        except Exception as ex:
            self.aLogger.error("getLastValueOfIIField.Error:" + str(ex))
            raise Exception("getLastValueOfIIField.Error:" + str(ex))

    def getProjectTree(self, pProjectName,pAssetGroupName):
        self.aLogger.info("getProjectTree:" + str(pProjectName) + "/" + str(pAssetGroupName))
        dfProject = self.getProjectDF(pProjectName)
        dProjectTree = dfProject.to_dict('records')[0]

        iProjectID = dProjectTree[f"{ProjectColumns.Project_Key}"]
        dfAssetGroup = self.getAssetGroupDF(pProjectName,iProjectID,pAssetGroupName)
        dAssetGroup = dfAssetGroup.to_dict('records')[0]
        dProjectTree["AssetGroup"] = dAssetGroup

        iAssetGroupID = dAssetGroup[f"{AssetGroupColumns.Asset_Group_Key}"]
        dfAssets = self.getAssetsDF(pProjectName, iAssetGroupID)
        lAssets = dfAssets.to_dict('records')
        lAssetsFinal = []
        for dAsset in lAssets:
            iAssetID = dAsset[f"{AssetColumns.Asset_Key}"]
            dfFields = self.getFieldsDF(pProjectName,iAssetID)
            lFields = dfFields.to_dict('records')
            dAsset["Fields"] = lFields
            lAssetsFinal.append(dAsset)

        dProjectTree["Assets"] = lAssetsFinal
        self.aLogger.info("getProjectTree.End")
        return dProjectTree

    def getProjectDF(self, pProjectName):
        self.aLogger.info("getProjectDF:" + str(pProjectName))
        spark = SparkSession.builder.appName(pProjectName).getOrCreate()
        sSQL = G_SQL_GetProject
        sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
        sSQL = sSQL.replace("<ProjectName>", pProjectName)
        self.aLogger.debug(sSQL)
        dfSpark = spark.read.jdbc(url=self.aEnvConfig["MetaData_URL"],
                                  table=sSQL,
                                  properties=self.aEnvConfig["MetaData_ConnProps"])
        dfPandas = dfSpark.select("*").toPandas()
        self.aLogger.info("getProjectDF.End")
        return dfPandas

    def getAssetGroupDF(self, pProjectName, pProjectID,pAssetGroupName):
        self.aLogger.info("getAssetGroupDF:" + str(pAssetGroupName))
        spark = SparkSession.builder.appName(pProjectName).getOrCreate()
        sSQL = G_SQL_GetAssetGroup
        sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
        sSQL = sSQL.replace("<ProjectID>", str(pProjectID))
        sSQL = sSQL.replace("<AssetGroupName>", str(pAssetGroupName))
        self.aLogger.debug(sSQL)
        dfSpark = spark.read.jdbc(url=self.aEnvConfig["MetaData_URL"],
                                  table=sSQL,
                                  properties=self.aEnvConfig["MetaData_ConnProps"])
        dfPandas = dfSpark.select("*").toPandas()
        self.aLogger.info("getAssetGroupDF.End")
        return dfPandas

    def getAssetsDF(self, pProjectName, pAssetGroupID):
        self.aLogger.info("getAssetsDF:" + str(pAssetGroupID))
        spark = SparkSession.builder.appName(pProjectName).getOrCreate()
        sSQL = G_SQL_GetAssets
        sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
        sSQL = sSQL.replace("<AssetGroupID>", str(pAssetGroupID))
        self.aLogger.debug(sSQL)
        dfSpark = spark.read.jdbc(url=self.aEnvConfig["MetaData_URL"],
                                  table=sSQL,
                                  properties=self.aEnvConfig["MetaData_ConnProps"])
        dfPandas = dfSpark.select("*").toPandas()
        dfPandas = dfPandas.sort_values(by=[f'{AssetColumns.Sequence_Number}'])
        self.aLogger.info("getAssetsDF.End")
        return dfPandas

    def getFieldsDF(self, pProjectName,pAssetID):
        self.aLogger.info("getFieldsDF:" + str(pAssetID))
        spark = SparkSession.builder.appName(pProjectName).getOrCreate()
        sSQL = G_SQL_GetFields
        sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
        sSQL = sSQL.replace("<AssetID>", str(pAssetID))
        self.aLogger.debug(sSQL)
        dfSpark = spark.read.jdbc(url=self.aEnvConfig["MetaData_URL"],
                                  table=sSQL,
                                  properties=self.aEnvConfig["MetaData_ConnProps"])
        dfSpark = dfSpark.orderBy(f'{FieldColumns.Field_Sequence_Number}')
        if f'{FieldColumns.Data_Protection_Code}' in dfSpark.columns:
            dfSpark = dfSpark.filter(dfSpark[f'{FieldColumns.Data_Protection_Code}'] != 'DoNotImport')
        dfPandas = dfSpark.select("*").toPandas()
        self.aLogger.info("getFieldsDF.End")
        return dfPandas




