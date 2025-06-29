import DP_Utility
from DP_Utility import *
import pandas as pd

class DIFMailer():
    # All DIF Errors must be defined here or in another location.
    # We should define a general convention of Error code formats of X.Y where X is a general catageory and Y a specific code.
    G_ErrorCodes = {
                "001.001":{"ShortName":"General.Test","Description":"This is just a test message"},
                "002.001":{"ShortName":"Metadata.NoAssets","Description":"There are no assets defined"},
                "010.001":{"ShortName":"FileValidation.General","Description":"General file validation errors"},
                    }
    
    def __init__(self,pEnvConfig, pAssetGroupConfig, pLogger):
        self.aEnvConfig = pEnvConfig
        self.aAssetGroupConfig = pAssetGroupConfig
        self.aLogger = pLogger

    def sendEmail(self,pTo,pSubject,pBody):
        try:
            self.aLogger.info("sendEmail.Start")
            DP_Emailer.sendAdminEmail(pTo,pSubject,pBody)
            return
        except Exception as ex:
            self.aLogger.error("sendEmail.Error: " + str(ex))
            raise Exception(str(ex))

    def raiseAlert(self,pJobTree,pAsset,pErrorCode, pLevel, pDetails):
        try:
            self.aLogger.info("RaiseAlert.Start")
            sSubject = self.buildSubject(pJobTree,pAsset,pErrorCode, pLevel,pDetails)  
            sBody = self.buildBody(pJobTree,pAsset,pErrorCode, pLevel, pDetails)
            self.sendEmail(self.aAssetGroupConfig["RunTime.Email"],sSubject,sBody)
            self.aLogger.info("raiseAlert.End")

        except Exception as ex:
            self.aLogger.error("raiseAlert.Error: " + str(ex))
            raise Exception(str(ex))
                            
    def buildSubject(self,pJobTree,pAsset,pErrorCode, pLevel, pDetails):
        sSubject = "DIF Alert:" + self.aEnvConfig["Environment"] + ":" + self.aEnvConfig["Node"] + ":" + str(pLevel)
        return sSubject
    
    def buildBody(self,pJobTree,pAsset,pErrorCode, pLevel, pDetails):
        sBody = "<B>DIF Alert</B><BR>"
        lInfo = []
        lInfo.append({"Setting":"Environment","Value":self.aEnvConfig["Environment"]})
        lInfo.append({"Setting":"Node","Value":self.aEnvConfig["Node"]})
        lInfo.append({"Setting":"Level","Value":pLevel})
        lInfo.append({"Setting":"ErrorCode","Value":pErrorCode})
        lInfo.append({"Setting":"ShortName","Value":self.getErrorCodeInfo(pErrorCode,"ShortName")})
        lInfo.append({"Setting":"Description","Value":self.getErrorCodeInfo(pErrorCode,"Description")})
        
        df = pd.DataFrame(lInfo)
        sBody =  sBody + df.to_html(index=False)
        sBody = sBody + self.buildDetails(pDetails)
        return sBody
    
    def buildDetails(self,pDetails):
        try:
            s = str(type(pDetails))
            # DataFrame
            if 'dataframe' in s.lower():
                return pDetails.to_html(index=False)

            # Dictionary
            if 'dict' in s.lower():
                sHTML = "<table><tr><td>Key</td><td>Value</td></tr>"
                for sKey in pDetails.keys():
                    sHTML = sHTML + "<tr><td>" + str(sKey) + "</td><td>" + str(pDetails[sKey]) + "</td></tr>"
                sHTML = sHTML + "</table>"
                return sHTML

            # list of dictionaries
            if 'list' in s:
                if 'dict' in str(type(pDetails[0])).lower():
                    df = pd.DataFrame(pDetails)
                    return str(df.to_html(index=False))

            # everything else
            return str(pDetails)
        except: 
            return str(pDetails)

    def getErrorCodeInfo(self,pErrorCode,pKey):
        try:
            return self.G_ErrorCodes[pErrorCode][pKey]
        except Exception as ex:
            self.aLogger.info("Error: " + str(ex))
            return "Unknown"
