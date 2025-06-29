import os
from DIFLogger import DIFLogger

class DIFBatchCSVHelper:
    def __init__(self, aSpark, aLogger, aEnvConfig, aAssetGroupConfig, asset):
        """
        Initialize the DIFBatchCSVHelper for processing CSV batch assets.
        Args:
            aSpark: The active Spark session
            aLogger: The logger to use for logging
            aEnvConfig: Environment configuration
            aAssetGroupConfig: Asset group configuration
            asset: The single asset to be processed
        """
        self.aSpark = aSpark
        self.aLogger = aLogger
        self.aEnvConfig = aEnvConfig
        self.aAssetGroupConfig = aAssetGroupConfig
        self.asset = asset

    def run(self):
        """
        This method processes a single CSV batch asset:
        - Reads the CSV files, processes them, and saves them as Parquet.
        """
        inputPattern = self.asset["batchIngestFilePattern"]  # Use the pattern from Batch_Group_String
        self.aLogger.info(f"Preprocessing CSV batch for asset: {self.asset['assetName']}")

        # Create the base staging path from Batch_Group_String and BatchID
        baseStagingPath = self.asset["batchBaseStagingPath"]  # Extracted from Batch_Group_String
        outputFolder = os.path.join(self.aEnvConfig["stagingRoot"], baseStagingPath, self.asset["batchId"])

        # Process CSV using the pattern
        df = self.aSpark.read.option("header", "true").csv(inputPattern)

        # Save the DataFrame as Parquet in the staging folder
        df.write.mode("overwrite").partitionBy("businessDate", "batchId").parquet(outputFolder)

        # Update the asset with the new staging Parquet path
        self.asset["ingestFile"] = outputFolder
        self.aLogger.info(f"CSV asset {self.asset['assetName']} preprocessed and saved to {outputFolder}")
