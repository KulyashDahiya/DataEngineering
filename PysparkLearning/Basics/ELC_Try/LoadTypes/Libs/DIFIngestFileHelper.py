# This needs to beome the single Ingest File helper - we have two - lets combine
# 
import datetime
from datetime import timedelta
import re
import zipfile
from pgpy import PGPKey, PGPMessage
import os
from DatabaseConstants import AssetColumns

# Need to replace ... key is kept near source for now
G_PGP_Passphrase = "ba"
G_PGP_PrivateKey_Path = "/Volumes/catalog_westeurope_dpn01_de_dev/db_fnd_ing/vol_ingest_fnd/DIFTest/Group01/2025/05/25/BAPrivateKey.asc"

class DIFIngestFileHelper():
    def __init__(self, pEnvConfig, pLogger, pSpark):
        self.aLogger = pLogger
        try:
            self.aLogger.info("DIFIngestFileHelper.Start")
            self.aEnvConfig = pEnvConfig
            self.aSpark = pSpark
        except Exception as ex:
            self.aLogger.error("DIFIngestFileHelper.Error:" + str(ex))
            raise Exception("DIFIngestFileHelper")
    def get_dbutils(self):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.aSpark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

    def fileMatchesPattern(self, pPattern,pFile):
        lMatches = re.findall(pPattern, pFile)
        if len(lMatches) == 0:
            return False
        return True
    
    # Find all files in a given Volume Folder that match a regex pattern
    def getFilesInFolder(self,pFolder,pFilePattern=""):
        try:
            self.aLogger.info("getFilesInFolder.Start")
            dbutils = self.get_dbutils()
            self.aLogger.info(str(pFolder) + ":" + str(pFilePattern))
            lMatchingFiles = []
            lfilesDirs = dbutils.fs.ls(pFolder)
            # Extracting just the names of files and directories
            lFiles = [file_info.name for file_info in lfilesDirs]
            
            for sFile in lFiles:
                if self.fileMatchesPattern(pFilePattern,sFile):
                    self.aLogger.debug("Pattern matches:" + sFile)
                    lMatchingFiles.append(pFolder + sFile)
                else:
                    self.aLogger.debug("Pattern does not match:" + sFile)
            self.aLogger.info("getFilesInFolder.End")
            return lMatchingFiles
        
        except Exception as ex:
            self.aLogger.info("getFilesInFolder.Error:" + str(ex))
            raise Exception("getFilesInFolder.Error:" + str(ex))

    # Given an Asset (F1F2, IngestFilePattern, BusinessDate, and Search Horizon
    def getIngestFilesWithinHorizon(self,pAsset,pCurrentBusinessDate):
        try:
            self.aLogger.info("getAssetFilesWithinHorizon.Start")
            lAllFiles = []
            dDate = datetime.datetime.strptime(pCurrentBusinessDate, "%Y-%m-%d").date()
            sDate = dDate.strftime("%Y-%m-%d")
            iCt = 0
            while iCt <= pAsset[f'{AssetColumns.Multiple_Files_Horizon_Number}']:
                sVolumePrefix = self.aEnvConfig["Volume_Ingest_"  + pAsset[AssetColumns.Container_Name]]
                sFolder = sVolumePrefix + pAsset[AssetColumns.Folder_Path_Name] + sDate.replace("-", "/") + "/"
                sFilePattern = pAsset[f'{AssetColumns.Ingest_File_Pattern_String}']
                lFiles = self.getFilesInFolder(sFolder, sFilePattern)
                lAllFiles = lFiles + lAllFiles
                iCt = iCt + 1
                dDate = dDate - timedelta(days=1)
                sDate = dDate.strftime("%Y-%m-%d")
            self.aLogger.info("getAssetFilesWithinHorizon.End")
            return lAllFiles
        except Exception as ex:
            self.aLogger.info("getAssetFilesWithinHorizon.Error:" + str(ex))
            raise Exception("getAssetFilesWithinHorizon.Error:" + str(ex))

    def extractFileFromZip(self,pZipFile, pEmbeddedFile,pBinary=False):
        try:
            self.aLogger.info("extractFileFromZip.Start")
            sTargetFolder = self.getFolderName(pZipFile)
            self.aLogger.debug(pZipFile + " to " + sTargetFolder + pEmbeddedFile)
            oZip = zipfile.ZipFile(pZipFile)
            lFiles = oZip.namelist()
            for sFile in lFiles:
                self.aLogger.debug("Found embedded file: " + str(sFile))
                if sFile == pEmbeddedFile:
                    self.aLogger.debug("Found expected file: " + str(sFile))
                    oZip.extract(pEmbeddedFile, path=sTargetFolder)
                    self.aLogger.info("extractFileFromZip.End")
                    return   
            raise Exception("Embedded file not found in zip")

        except Exception as ex:
            sMsg = "extractFileFromZip.Error:"+str(ex)
            self.aLogger.error(sMsg)
            raise Exception(sMsg)

    def decryptFile_OLD(self,pEncryptedFile,pClearFile,pBinary=False):
        try:
            self.aLogger.info("decryptFile.Start: " + pEncryptedFile + " to " + pClearFile) 
            msg = PGPMessage()
            emsg = msg.from_file(pEncryptedFile)
            privkey, _ = PGPKey.from_file(G_PGP_PrivateKey_Path)
            with privkey.unlock("ba"):
                data = privkey.decrypt(emsg)
                if not data.is_encrypted:
                    if pBinary:
                        with open(pClearFile, "wb") as write_file:
                            write_file.write(data.message)
                    else:
                        with open(pClearFile, "wt") as write_file:
                            write_file.write(str(data.message))
                else:
                    raise Exception("Can not decrypt: " + pEncryptedFile) 

        except Exception as ex:
            self.aLogger.error("decryptFile.Error: " + str(ex))
            raise Exception("decryptFile.Error: " + str(ex))

    def decryptFile(self,pEncryptedFile,pClearFile,pNodeInfo,pBinary=False):
        #TODO: NodeInfo can not be hardcoded Node01 comes from EnvConfig; Date comes from Asset
        sPGPKeyText = self.aEnvConfig["PrivateKey"+pNodeInfo]
        sPGPPassword = self.aEnvConfig["PGPKeyPassword"+pNodeInfo]
        try:
            self.aLogger.info("DIFIngestFileHelper.decryptFile.Start: " + pEncryptedFile + " to " + pClearFile) 
            msg = PGPMessage()
            emsg = msg.from_file(pEncryptedFile)
            privkey, _ = PGPKey.from_blob(sPGPKeyText)
            with privkey.unlock(sPGPPassword):
                data = privkey.decrypt(emsg)
                if not data.is_encrypted:
                    if pBinary:
                        with open(pClearFile, "wb") as write_file:
                            write_file.write(data.message)
                    else:
                        with open(pClearFile, "wt") as write_file:
                            write_file.write(str(data.message))
                else:
                    raise Exception("Can not decrypt: " + pEncryptedFile) 
            self.aLogger.info("DIFIngestFileHelper.decryptFile.End")
        except Exception as ex:
            self.aLogger.error("DIFIngestFileHelper.decryptFile.Error: " + str(ex))
            raise Exception("DIFIngestFileHelper.decryptFile.Error: " + str(ex))

    def getRootFolder(self,pFile):
        sRootFolder = "/".join(pFile.split("/")[0:-1])
        return sRootFolder

    def getFileShortName(self,pFullFileName):
        print(pFullFileName.split("/")[-1])

    # We always know the IngestFileName
    def getEncryptedFileName(self,pAsset):
        if pAsset[f'{AssetColumns.Decrypt_Indicator}'] == "Y":
            return pAsset["IngestFile"]
        return "None"

    def getZipFileName(self,pAsset):
        try:
            self.aLogger.debug("DIFFileUtility.getZipFileName.Start")
            if pAsset[f'{AssetColumns.Unzip_Indicator}'] != "Y":
                return "None"

            sZipFileName = pAsset["IngestFile"]
            if pAsset[f'{AssetColumns.Decrypt_Indicator}'] == "Y":
                sPGPExt = "." + pAsset["EncryptedFile"].split(".")[1]
                sZipFileName =  pAsset["IngestFile"].replace(sPGPExt, ".zip")

            self.aLogger.debug("DIFFileUtility.getZipFileName.End")
            return sZipFileName

        except Exception as ex:
            self.aLogger.error("DIFFileUtility.getZipFileName.Error:"+str(ex))
            raise Exception(ex)

    # DecryptedFIleName = IngestFileName.zip or .ext
    def getDecryptedFileName(self, pAsset):
        try:
            self.aLogger.debug("DIFFileUtility.getDecryptedFileName.Start")

            if pAsset[f'{AssetColumns.Decrypt_Indicator}'] != "Y":
                self.aLogger.debug("DIFFileUtility.getDecryptedFileName.End")
                return "None"

            sDecryptedFileName = "Unknown"
            sPGPExt = "." + pAsset["IngestFile"].split(".")[1]
            if pAsset[f'{AssetColumns.Unzip_Indicator}'] == "Y":
                sExt = ".zip"
            else:
                sExt = "." + pAsset[f'{AssetColumns.Embedded_File_Name}'].split(".")[1]

            sDecryptedFileName = pAsset["IngestFile"].replace(sPGPExt,sExt)
            self.aLogger.debug("DIFFileUtility.getDecryptedFileName.End:" + sDecryptedFileName)
            return sDecryptedFileName

        except Exception as ex:
            self.aLogger.error("DIFFileUtility.getDecryptedFileName.End.Error:"+str(ex))
            raise Exception(ex)

    def getDataFileName(self,pAsset):
        if pAsset[f'{AssetColumns.Unzip_Indicator}'] == "Y":
            return pAsset["IngestFolder"] + "/" + pAsset[f'{AssetColumns.Embedded_File_Name}']

        if pAsset[f'{AssetColumns.Decrypt_Indicator}'] == "Y":
            return pAsset["DecryptedFile"]

        return pAsset["IngestFile"]

    #PATH/F1/F2/YYYY/MM/DD/FileNamee.ext
    def getRawFolder(self,pAsset):
        sF1F2 = pAsset[f'{AssetColumns.Folder_Path_Name}']
        self.aLogger.info("getRawFolder for:" + pAsset[f"{AssetColumns.Asset_Name}"])

        sContainer = pAsset[f'{AssetColumns.Container_Name}']
        sRawFolder = self.aEnvConfig["RawPathABFSS_" + sContainer] + sF1F2 + "/" + pAsset[f'{AssetColumns.Asset_Name}'] + "/"

        self.aLogger.info(sRawFolder)
        return sRawFolder

    #returns 2024-12-31
    def getBusinessDate(self,pAsset):
        try:
            self.aLogger.debug("DIFFileUtility.getBusinessDate.Start")
            sBusinessDate = self.getDatePartOfPath(pAsset)
            sBusinessDate = sBusinessDate.replace("/","-")
            self.aLogger.debug("DIFFileUtility.getBusinessDate.End")
            return sBusinessDate
        except Exception as ex:
            self.aLogger.error("DIFFileUtility.getBusinessDate.Error:" + str(ex))
            raise Exception(ex)

    #Returns /2024/12/31
    def getDatePartOfPath(self,pAsset):
        try:
            self.aLogger.debug("DIFFileUtility.getDatePartOfPath.Start")
            self.aLogger.debug("FolderPath:"+pAsset[f'{AssetColumns.Folder_Path_Name}'])
            self.aLogger.debug("IngestFileName:"+pAsset["IngestFile"])
            sF1F2 = pAsset[f'{AssetColumns.Folder_Path_Name}']
            sFileName = pAsset["IngestFile"]

            i = sFileName.find(sF1F2)
            iLength = len(sF1F2)
            self.aLogger.debug(str(i) + "," + str(iLength))

            sDatePart = sFileName[i + iLength:i + iLength + 10]
            self.aLogger.debug("DatePart:" + sDatePart)
            self.aLogger.debug("DIFFileUtility.getDatePartOfPath.End")
            return sDatePart

        except Exception as ex:
            self.aLogger.error("DIFFileUtility.getDatePartOfPath.Error:" + str(ex))
            raise Exception(ex)

    def getRawFolderWithDate(self,pAsset):
        try:
            self.aLogger.info("DIFFileUtility.getRawFolderWithDate for:" + pAsset[f"{AssetColumns.Asset_Name}"])
            sF1F2 = pAsset[f'{AssetColumns.Folder_Path_Name}']
            sAssetName = pAsset[f"{AssetColumns.Asset_Name}"]
            sContainer = pAsset[f'{AssetColumns.Container_Name}']
            sPathToRaw = self.aEnvConfig["RawPathABFSS_" + sContainer]
            self.aLogger.info("PathToRaw: " + sPathToRaw)

            sDatePart = self.getDatePartOfPath(pAsset)
            self.aLogger.info("DatePart: " + sDatePart)

            sRawFolder = sPathToRaw + sF1F2 + sDatePart + "/"
            self.aLogger.info("RawFolder: " + sRawFolder)
            self.aLogger.info("DIFFileUtility.getRawFolderWithDate.End")
            return sRawFolder

        except Exception as ex:
            self.aLogger.error("DIFFileUtility.getRawFolderWithDate.Error:" + str(ex))
            raise Exception(ex)

    def getShortFileName(self,pFileName):
        return pFileName.split("/")[-1] 
    
    def getFolderName(self,pFileName):
        return pFileName.replace(self.getShortFileName(pFileName),'')
    

    

