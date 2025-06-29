import os
from DIFLogger import DIFLogger

class DIFMultiAssetBatchHelper:
    def __init__(self, aSpark, aLogger, aEnvConfig, aAssetGroupConfig, groupId, groupAssets):
        """
        Initialize the DIFMultiAssetBatchHelper for handling multi-asset batch.
        Args:
            aSpark: The active Spark session
            aLogger: The logger to use for logging
            aEnvConfig: Environment configuration
            aAssetGroupConfig: Asset group configuration
            groupId: The BatchAssetGroup number for grouping
            groupAssets: The list of assets within this batch group
        """
        self.aSpark = aSpark
        self.aLogger = aLogger
        self.aEnvConfig = aEnvConfig
        self.aAssetGroupConfig = aAssetGroupConfig
        self.groupId = groupId
        self.groupAssets = groupAssets

    def run(self):
        """
        Processes multi-asset batches:
        - Preprocesses shared input files for the batch group.
        - Stages the processed data into separate Parquet files.
        """
        inputPattern = self.groupAssets[0]["batchInputPattern"]  # All batch assets share the same input pattern

        self.aLogger.info(f"Preprocessing multi-asset batch group {self.groupId}")

        for asset in self.groupAssets:
            # Placeholder for logic to process multi-asset batches
            self.aLogger.info(f"Processing multi-asset batch for asset {asset['assetName']}")
            self.processBatchAsset(asset, inputPattern)

    def processBatchAsset(self, asset, inputPattern):
        """
        Processes a single asset within a multi-asset batch.
        Reads the raw input files, normalizes them, and saves them to the appropriate Parquet folder.
        """
        self.aLogger.info(f"Processing asset {asset['assetName']} in batch group {self.groupId}")

        outputFolder = os.path.join(self.aEnvConfig["stagingRoot"], asset["assetName"], self.groupId)
        df = self.aSpark.read.json(inputPattern)  # Assuming JSON as the source format
        # Add any normalization here if required
        df.write.mode("overwrite").partitionBy("businessDate", "batchId").parquet(outputFolder)

        asset["ingestFile"] = outputFolder
        self.aLogger.info(f"Asset {asset['assetName']} preprocessed and saved to {outputFolder}")
