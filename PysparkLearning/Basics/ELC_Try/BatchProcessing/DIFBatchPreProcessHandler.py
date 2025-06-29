import os
import datetime
from DIFLogger import DIFLogger
import DIFIngestFileHelper as DIFIngestFileHelper
from DIFBatchCSVHelper import DIFBatchCSVHelper
from DIFBatchParquetHelper import DIFBatchParquetHelper
from DIFBatchJsonHelper import DIFBatchJsonHelper  # Placeholder for JSON handling


class DIFBatchPreProcessHandler:
    def __init__(self, aSpark, aLogger, aEnvConfig, aAssetGroupConfig, dJobTree):
        """
        Initialize the DIFBatchPreProcessHandler.
        Args:
            aSpark: The active Spark session
            aLogger: The logger to use for logging
            aEnvConfig: Environment configuration
            aAssetGroupConfig: Asset group configuration
            dJobTree: The dJobTree containing asset and batch configurations
        """
        self.aSpark = aSpark
        self.aLogger = aLogger
        self.aEnvConfig = aEnvConfig
        self.aAssetGroupConfig = aAssetGroupConfig
        self.dJobTree = dJobTree

    def run(self, pCurrentBusinessDate):
        """
        This method processes the assets in the dJobTree:
        - Moves the batch files to the BatchID folder.
        - Processes each asset and stores the DataFrame as Parquet in the Ingest Date folder.
        """
        dDate = datetime.datetime.strptime(pCurrentBusinessDate, "%Y-%m-%d").date()
        sDate = dDate.strftime("%Y-%m-%d")

        # Loop over assets to handle batch-enabled ones
        for dAsset in self.dJobTree["Assets"]:
            try:
                # Check if the asset is batch-enabled
                batchGroupString = dAsset.get("Batch_Group_String")
                if batchGroupString is None:
                    continue  # Skip assets that are not batch-enabled

                # Retrieve the IngestFile Pattern (e.g., "Employee_*.csv")
                sFilePattern = dAsset.get(f'{AssetColumns.Ingest_File_Pattern_String}')
                if not sFilePattern:
                    self.aLogger.warning(
                        f"Asset {dAsset[f'{AssetColumns.Asset_Name}']} does not have a valid IngestFilePattern. Skipping.")
                    continue

                # Step 1: Move files to BatchID folder under Batch
                batchID = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")

                sVolumePrefix = self.aEnvConfig["Volume_Ingest_" + dAsset[AssetColumns.Container_Name]]
                sFolder = sVolumePrefix + dAsset[AssetColumns.Folder_Path_Name] + sDate.replace("-", "/") + "/"

                sBatchFolderPath = os.path.join(sFolder, "Batch", batchID)

                # Move files to pre-staging
                oFileHelper = DIFIngestFileHelper.DIFIngestFileHelper(self.aEnvConfig, self.aLogger, self.aSpark)
                lFiles = oFileHelper.getIngestFilesWithinHorizon(dAsset, pCurrentBusinessDate)
                self.moveFilesToPreStaging(lFiles, sBatchFolderPath)

                # Step 2: Process each asset based on IngestFileType
                self.processAssetFiles(dAsset, sBatchFolderPath, batchID, sFolder)

            except Exception as e:
                self.aLogger.error(f"Error processing asset {dAsset[f'{AssetColumns.Asset_Name}']}: {str(e)}")
                continue  # Skip this asset and continue with the rest

        return

    def processAssetFiles(self, dAsset, preStagingFolder, batchID, sFolder):
        """
        Process each asset's files based on its IngestFileType and save them as Parquet.
        """
        assetType = dAsset.get("ingestFileType", "").lower()

        try:
            if "csv" in assetType:
                # Process CSV files
                self.aLogger.info(f"Processing CSV for asset: {dAsset[f'{AssetColumns.Asset_Name}']}")
                self.processCSVAsset(preStagingFolder, batchID, dAsset, sFolder)
            elif "parquet" in assetType:
                # Process Parquet files
                self.aLogger.info(f"Processing Parquet for asset: {dAsset[f'{AssetColumns.Asset_Name}']}")
                oPH = DIFBatchParquetHelper(self.aSpark, self.aLogger, self.aEnvConfig, self.aAssetGroupConfig, dAsset)
                oPH.run(preStagingFolder, batchID)
            elif "json" in assetType:
                # Placeholder for JSON handling, call DIFBatchJsonHelper
                self.aLogger.info(f"Processing JSON for asset: {dAsset[f'{AssetColumns.Asset_Name}']} - Placeholder for future logic.")
                oJSONH = DIFBatchJsonHelper(self.aSpark, self.aLogger, self.aEnvConfig, self.aAssetGroupConfig, dAsset)
                oJSONH.run(preStagingFolder, batchID)
            else:
                self.aLogger.warning(f"Unsupported file type for asset {dAsset[f'{AssetColumns.Asset_Name}']}: {assetType}")
                raise ValueError(f"Unsupported file type for asset {dAsset[f'{AssetColumns.Asset_Name}']}")
        except Exception as e:
            self.aLogger.error(f"Error processing asset {dAsset[f'{AssetColumns.Asset_Name}']} files: {str(e)}")
            raise

    def processCSVAsset(self, preStagingFolder, batchID, dAsset, sFolder):
        """
        Processes a single CSV asset:
        - Reads CSV files from Pre-Staging.
        - Saves them as Parquet in the Staging folder.
        """
        inputPattern = os.path.join(preStagingFolder, dAsset["batchIngestFilePattern"])  # Path to CSV files
        self.aLogger.info(f"Preprocessing CSV for asset: {dAsset[f'{AssetColumns.Asset_Name}']} using pattern: {inputPattern}")

        # Construct the Parquet save path
        outputFolder = os.path.join(sFolder, f"{dAsset[f'{AssetColumns.Asset_Name}']}_{batchID}.parquet")

        # Step 1: Read the CSV files into a DataFrame
        df = self.aSpark.read.option("header", "true").csv(inputPattern)

        # Step 2: Write the DataFrame as Parquet in the Staging folder (no partitioning)
        self.aLogger.info(f"Saving Parquet for asset: {dAsset[f'{AssetColumns.Asset_Name}']} to {outputFolder}")
        df.write.mode("overwrite").parquet(outputFolder)

        # Update the asset with the new Parquet path
        dAsset["ingestFile"] = outputFolder
        self.aLogger.info(f"CSV asset {dAsset[f'{AssetColumns.Asset_Name}']} preprocessed and saved to {outputFolder}")

    def moveFilesToPreStaging(self, lFiles, preStagingFolder):
        """
        This method moves the list of files to the pre-staging folder.
        """
        if not os.path.exists(preStagingFolder):
            os.makedirs(preStagingFolder)

        for file in lFiles:
            try:
                destPath = os.path.join(preStagingFolder, os.path.basename(file))
                os.rename(file, destPath)  # Move file to pre-staging folder
                self.aLogger.info(f"Moved {file} to {destPath}")
            except Exception as e:
                self.aLogger.error(f"Error moving file {file}: {e}")