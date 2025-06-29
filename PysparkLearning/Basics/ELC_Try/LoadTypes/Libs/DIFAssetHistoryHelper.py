# All functions dealing with file history and status
import pyodbc
import os
import sys
import time
import datetime as dt

# Add DIF Libraries
from DatabaseConstants import TableNames,ProjectColumns,AssetGroupColumns,AssetColumns,FieldColumns,IncrementalLoadColumns,AssetHistoryColumns,AssetStatusColumns,HistoricalLoadColumns,HolidayCalendarColumns

G_SQLSelectMaxFileForStep = f"""Select Max({AssetHistoryColumns.File_Name}) as MaxFileName from <DIFSCHEMA>.{TableNames.AssetHistory}  
        where {AssetHistoryColumns.Asset_Key}=<AssetID_FK>  and {AssetHistoryColumns.Step_Name}='<Step>' and {AssetHistoryColumns.Status_Code}='OK'"""

G_SQLSelectRowCount = f"""Select Count(*) as CT from <DIFSCHEMA>.{TableNames.AssetHistory}  
        where {AssetHistoryColumns.Asset_Key}=<AssetID_FK>  and {AssetHistoryColumns.File_Name}='<FileName>' 
        and {AssetHistoryColumns.Step_Name}='<Step>' and {AssetHistoryColumns.Status_Code}='OK'"""

G_SQLInsert = f"""Insert into <DIFSCHEMA>.{TableNames.AssetHistory} 
    ({AssetHistoryColumns.Asset_Key},{AssetColumns.Asset_Name},{AssetHistoryColumns.Business_Date},
    {AssetHistoryColumns.Run_Identifier},{AssetHistoryColumns.File_Name},{AssetHistoryColumns.Step_Name},
    {AssetHistoryColumns.Status_Code},{AssetHistoryColumns.Status_Details_String},
    {AssetHistoryColumns.Step_Details_String})
    Values
    (<AssetID_FK>,'<AssetName>','<BusinessDate>','<RunID>','<FileName>','<Step>','<Status>','<StatusDetails>','<StepDetails>') """

class DIFAssetHistoryHelper:
    def __init__(self, pEnvConfig, pAssetGroupConfig, pLogger, pAsset):
        self.aEnvConfig = pEnvConfig
        self.aAssetGroupConfig = pAssetGroupConfig
        self.aLogger = pLogger
        self.aAsset = pAsset

    def isOutOfSequence(self):
        try:
            return False
            self.aLogger.debug("isOutOfSequence.Start")
            if self.aAsset[f'{AssetColumns.Multiple_Files_Sequence_Check_Indicator}'] == "N":
                self.aLogger.debug("Sequence check not needed")
                return False

            sLastFileName = self.getLastCompletedFileName()
            self.aLogger.debug("LastFile:" + str(sLastFileName))
            self.aLogger.debug("ThisFile:" + str(self.aAsset["IngestFile"]))

            if sLastFileName is None:
                self.aLogger.debug("Last date is None ... never checked")
                return False

            if sLastFileName > self.aAsset["IngestFile"]:
                self.aLogger.debug("Out Of Sequence is TRUE")
                return True

            self.aLogger.debug("Out Of Sequence is FALSE")
            return False

        except Exception as ex:
            self.aLogger.error("isOutOfSequence.End.Error: " + str(ex))
            raise Exception(str(ex))

    # Is this Step marked as complete?
    # Steps are always X.Start or X.End
    def hasStepCompleted(self,pStep):
        try:
            sSQL = G_SQLSelectRowCount
            sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
            sSQL = sSQL.replace("<AssetID_FK>", str(self.aAsset[f'{AssetColumns.Asset_Key}']))
            try:
                sSQL = sSQL.replace("<FileName>", self.aAsset["IngestFile"])
            except:
                sSQL = sSQL.replace("<FileName>", "None")
            sSQL = sSQL.replace("<Step>", pStep)
            self.aLogger.debug(sSQL)
            oConn = self.getSQLConnection()
            oCur = oConn.cursor()
            oCur.execute(sSQL)
            lRows = oCur.fetchall()
            iCt = lRows[0][0]
            if iCt == 0:
                return False
            self.aLogger.info("hasStepCompleted.End")
            return True
        except Exception as ex:
            self.aLogger.error("hasStepCompleted.Error:" + str(ex))
            raise Exception(ex)

    def insertStep(self,pStep,pStatus,pStatusDetails="",pStepDetails=""):
        self.aLogger.info("insertStep.Start")
        try:
            sSQL = G_SQLInsert
            sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
            sSQL = sSQL.replace("<AssetID_FK>", str(self.aAsset[f'{AssetColumns.Asset_Key}']))
            sSQL = sSQL.replace("<AssetName>", self.aAsset[f"{AssetColumns.Asset_Name}"])
            try:
                sSQL = sSQL.replace("<FileName>", self.aAsset["IngestFile"])
            except:
                sSQL = sSQL.replace("<FileName>", "None")

            sSQL = sSQL.replace("<BusinessDate>", self.aAssetGroupConfig["BusinessDate"])
            sSQL = sSQL.replace("<RunID>", self.aAssetGroupConfig["RunID"])
            sSQL = sSQL.replace("<Step>", pStep)
            sSQL = sSQL.replace("<Status>", pStatus)
            sSQL = sSQL.replace("<StatusDetails>", pStatusDetails)
            sSQL = sSQL.replace("<StepDetails>", pStepDetails)

            oConn = self.getSQLConnection()
            oCur = oConn.cursor()
            self.aLogger.debug(sSQL)
            oCur.execute(sSQL)
            oConn.commit()
            oCur.close()
            oConn.close()
            self.aLogger.info("insertStep.End.OK")

        except Exception as ex:
            self.aLogger.error("insertStep.End.Error: " + str(ex))
            raise Exception(str(ex))

    def getSQLConnection(self):
        self.aLogger.debug("getSQLConnection.Start")
        try:
            oConn = pyodbc.connect(self.aEnvConfig["SQLConnString"])
            return oConn
        except Exception as ex:
            self.aLogger.error("getSQLConnection.Error: " + str(ex))
            raise Exception(str(ex))

    def getFileDate(self):
        file_time = dt.datetime.fromtimestamp(os.path.getmtime(self.aAsset["CurrentFileName"]))
        sDateTime = file_time.strftime("%Y%m%d %H:%M:%S")
        return sDateTime

    # This check can really only be used if the sort mode is based on filename
    # Since by definition a file that comes in after the prior group is never out of sequence


    def getLastCompletedFileName(self):
        sSQL = G_SQLSelectMaxFileForStep
        sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
        sSQL = sSQL.replace("<AssetID_FK>", str(self.aAsset[f'{AssetColumns.Asset_Key}']))
        sSQL = sSQL.replace("<Step>", 'IngestToRaw.End')
        oConn = self.getSQLConnection()
        oCur = oConn.cursor()
        self.aLogger.debug(sSQL)
        oCur.execute(sSQL)
        lRows = oCur.fetchall()
        if len(lRows) > 0:
            sFileName = lRows[0][0]
        else:
            sFileName = None

        oCur.close()
        oConn.close()
        return sFileName



