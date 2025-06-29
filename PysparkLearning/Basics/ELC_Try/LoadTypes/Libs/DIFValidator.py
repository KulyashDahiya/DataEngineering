import json
import pandas as pd
from io import StringIO
from pyspark.sql import functions as F
from itertools import chain

class DIFValidator:
    """Handles data validation rules."""

    def __init__(self, pEnvConfig, pLogger, pAssetGroupConfig):
        """Initializes the validator with environment configurations and a logger."""
        # Store the passed configurations and logger
        self.pEnvConfig = pEnvConfig
        self.pLogger = pLogger
        self.pAssetGroupConfig = pAssetGroupConfig
        
    def testMaxValue(self, pDF, pRule):
        """Test max value validation."""
        try:
            oResult = {'ReturnCode': 0, 'Messages': [],'Warning': []}
            sField = pRule['Field']
            xValue = pRule['Value']
            # Check for rows where the value in the field exceeds the max value
            dfErrors = pDF.filter(pDF[sField] > xValue)
            if dfErrors.count() > 0:
                oResult['ReturnCode'] = 1
                oResult['Messages'].append(f"Max value not reached for {sField} column")
                if pRule['Severity'] == 'Warning':
                    self.pLogger.info(f"Email notification sent successfully for {sField}")   
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during max value validation: {e}")
            raise
    
    def testNoNulls(self, pDF, pRule):
        """Test for null values in specified fields."""
        try:
            oResult = {}
            listErrors = []
    
            # Init
            oResult['ReturnCode'] = 0
            ctBefore = 0
            ctAfter = 0

            # Run test
            listFields = pRule['FieldList']
            ctBefore = pDF.count()
            ctAfter = pDF.dropna(how='any', subset=listFields).count()
   
            # Check if any rows were dropped due to null values
            if ctAfter < ctBefore:
                oResult['ReturnCode'] = 1
                listErrors.append(f"Nulls found for {listFields} column")
                if pRule['Severity'] == 'Warning':
                    self.pLogger.info(f"Email notification sent successfully") 
            oResult['Messages'] = listErrors  # Assign the list of errors to 'Messages'  
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during NoNull validation: {e}")
            raise

    def testAllowedValues(self,pDF,pRule):
        """Test that the values in a field are among allowed values."""
        try:
            # Init
            oResult = {}
            listErrors = []
            listErrorRows = []
            oResult['ReturnCode'] = 0
        
            # Get Rule Settings
            sField = pRule['Field']
            listValues = pRule['Values']
            dfGood = pDF.filter(pDF[sField].isin(listValues))
            missingValues = set(listValues) - set([row[sField] for row in dfGood.collect()])
            if len(missingValues) > 0:
                oResult['ReturnCode'] = 1
                listErrors.append(f"Failed test case, missing values: {missingValues}")
                if pRule['Severity'] == 'Warning':
                    self.pLogger.info(f"Email notification sent successfully")   

            # Report Results - 0 means no errors found, 1 means error found
            oResult['Messages'] = listErrors
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during AllowedValues validation: {e}")
            raise
    
    def testMinValue(self, pDF, pRule):
        """Validate that the values in a specified column meet a minimum threshold."""
        try:
            oResult = {'ReturnCode': 0, 'Messages': []}
            #sField = pRule['Field']  # Field to validate
            sField = pRule['Field']
            xValue = pRule['Value']  # Minimum value threshold  
            dfErrors = pDF.filter(pDF[sField] < xValue)
            if dfErrors.count() > 0:
                oResult['ReturnCode'] = 1
                oResult['Messages'].append(f"Min value not reached for {sField} column")
                if pRule['Severity'] == 'Warning':
                    self.pLogger.info(f"Email notification sent successfully")  
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during min value validation: {e}")
            raise

    def testMinRowValue(self,pDF,pRule):
        """Test the minimum row count for a dataset."""
        try:
            # Init
            oResult = {}
            listErrors = []
            listErrorRows = []
            oResult['ReturnCode'] = 0

            # Get Rule Settings
            iMin = pRule["Value"]

            # get file statistics
            ct = pDF.count()
        
            # check Min Rows
            if ct < iMin:
                oResult['ReturnCode'] = 1
                listErrors.append("Minimum column count failed. Minimum = " + str(iMin) + ". Found=" + str(ct))
                if pRule['Severity'] == 'Warning':
                    self.pLogger.info(f"Email notification sent successfully")
           
            # Report Results - 0 means no errors found, 1 means error found
            oResult['Messages'] = listErrors
            oResult['RowsInError'] = listErrorRows
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during MinRowValue validation: {e}")
            raise
        
    
    def testMaxRowValue(self,pDF,pRule):
        """Test the maximum row count for a dataset."""
        try:
            # Init
            oResult = {}
            listErrors = []
            listErrorRows = []
            oResult['ReturnCode'] = 0

            # Get Rule Settings
            iMax = pRule["Value"]

            # get file statistics
            ct = pDF.count()
        
            # check Min Rows
            if iMax > 0 and ct > iMax:
                oResult['ReturnCode'] = 1
                listErrors.append("Maximum row count failed. Maximum = " + str(iMax) + ". Found=" + str(ct))
                if pRule['Severity'] == 'Warning':
                    self.pLogger.info(f"Email notification sent successfully")
           
            # Report Results - 0 means no errors found, 1 means error found
            oResult['Messages'] = listErrors
            oResult['RowsInError'] = listErrorRows
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during MaxRowValue validation: {e}")
            raise
    
    def testRequiredFields(self,pDF,pRule):
        """Test if all required fields exist in the dataset."""
        try:
            # Init
            oResult = {}
            listErrors = []
            oResult['ReturnCode'] = 0

            # Check for missing fields
            listFields = pRule['FieldList']
            actualColumns = pDF.columns
            for sField in listFields:
                if not sField in actualColumns:
                    oResult['ReturnCode'] = 1
                    listErrors.append("Required field is missing: " + sField)
                    if pRule['Severity'] == 'Warning':
                        self.pLogger.info(f"Email notification sent successfully")       
           
            # Report Results - 0 means no errors found, 1 means error found, 2 means execution error
            oResult['Messages'] = listErrors
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during RequiredFields validation: {e}")
            raise
    
    def testAllowDuplicates(self,pDF,pRule):
        """Test if duplicates are allowed in the dataset."""
        try:
            # Init
            oResult = {}
            listErrors = []
            oResult['ReturnCode'] = 0

            # Get Rule Settings
            listFields = pRule['FieldList']
            lPkChecks=pRule['Value']
            if lPkChecks == 'Y':
                ctBefore = pDF.count()
                ctAfter = pDF.drop_duplicates(subset=listFields).count()
                # check Min Rows
                if ctAfter < ctBefore:
                    oResult['ReturnCode'] = 1
                    listErrors.append("Duplicate rows were found")
                    if pRule['Severity'] == 'Warning':
                        self.pLogger.info(f"Email notification sent successfully")

            # Report Results - 0 means no errors found, 1 means error found
            oResult['Messages'] = listErrors
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during AllowDuplicates validation: {e}")
            raise
        
    
    def testAllowBlankPK(self,pDF,pRule):
        """Test if blank primary key values are allowed."""
        try:
            oResult = {}
            listErrors = []
            oResult['ReturnCode'] = 0
            # Get Rule Settings
            listFields = pRule['FieldList']
            lPkChecks=pRule['Value']
            if lPkChecks == 'Y':
                # Create a condition that checks for blank or null values in any of the primary columns
                condition = None
                for col in listFields:
                    # Condition to check for null or blank
                    if condition is None:
                        condition = (F.col(col).isNull()) | (F.col(col) == '')
                    else:
                        condition = condition | (F.col(col).isNull()) | (F.col(col) == '')    

                # Check if any rows match the condition (blank or null values in any primary column)
                blank_or_null_rows = pDF.filter(condition)
                # If any rows with blank or null values are found
                if blank_or_null_rows.count() > 0:
                    oResult['ReturnCode'] = 1
                    listErrors.append("Found blank rows were found")
                    if pRule['Severity'] == 'Warning':
                        self.pLogger.info(f"Email notification sent successfully")
            # Report Results - 0 means no errors found, 1 means error found
            oResult['Messages'] = listErrors
            return oResult
        except Exception as e:
            self.pLogger.error(f"Error: Failed during AllowBlankPK validation: {e}")
            raise
    
    def getMissingParameters(self, pRule, pListParameters):
        """Get missing parameters for a test rule."""
        listMissingParameters = []
        for sParameter in pListParameters:
            if not sParameter in pRule:
                listMissingParameters.append(sParameter)
        return listMissingParameters
    
    def executeTests(self, pData, listTestsToExecute,passet):
        """Execute a list of tests on the data."""
        listStandardTests = [
        {"Type":"Asset","RuleName":"MinRows","funct": self.testMinRowValue, "required": ["Value","Severity"]},
        {"Type":"Asset","RuleName":"MaxRows","funct": self.testMaxRowValue, "required": ["Value","Severity"]},
        {"Type":"Asset","RuleName":"RequiredFields","funct": self.testRequiredFields, "required": ["FieldList","Severity"]},
        {"Type":"Asset","RuleName":"AllowDuplicates","funct": self.testAllowDuplicates, "required": ["FieldList","Value","Severity"]},
        {"Type":"Asset","RuleName":"AllowBlankPk","funct": self.testAllowBlankPK, "required": ["FieldList","Value","Severity"]},
        {"Type":"Field","RuleName":"NoNulls","funct": self.testNoNulls, "required": ["FieldList","Severity"]},
        {"Type":"Field","RuleName":"Minimum","funct": self.testMinValue, "required": ["Field", "Value","Severity"]},
        {"Type":"Field","RuleName":"Maximum","funct": self.testMaxValue, "required": ["Field", "Value","Severity"]},
        {"Type":"Field","RuleName":"AllowedValues","funct": self.testAllowedValues, "required": ["Field", "Values","Severity"]}
        ]
    
        oOverallResults = {}
        iOverallReturnCode = 0
        oOverallTestDetails = []
        oTestResult = {}
        listMissingParameters = []
        oResult = {}
        oResult['ReturnCode'] = 0

        # Iterate over tests to execute
        for oTest in listTestsToExecute:
            sTestType = oTest["TestType"]

            # Create a vTestRule dictionary to preserve the Validation values
            vTestRule = {key: value for key, value in oTest.items()}

            # Find the test definition in listStandardTests based on RuleName
            testDefinition = next((test for test in listStandardTests if test["RuleName"] == sTestType), None)

            # Handle special cases for RequiredFields and AllowDuplicates/AllowBlankPk rules
            if testDefinition["RuleName"]== 'RequiredFields':
                listsourcename=[]
                for field in passet['Fields']:
                    SourceFieldName = field.get('Source_Field_Name', '')
                    listsourcename.append(SourceFieldName)
                    oTest['FieldList']=listsourcename
            elif testDefinition["RuleName"]== 'AllowDuplicates' or  testDefinition["RuleName"]== "AllowBlankPk":  
                listsourcename=[]
                for field in passet['Fields']:
                    if field.get('IsKey_Indicator', '') == 'Y':
                        SourceFieldName = field.get('Source_Field_Name', '')
                        listsourcename.append(SourceFieldName)
                        oTest['FieldList']=listsourcename  
                    

            # Check for missing parameters
            listMissingParameters = self.getMissingParameters(
                oTest, testDefinition["required"]
            )
        
            # If missing parameters, return error
            if len(listMissingParameters) > 0:
                oResult["ReturnCode"] = 1
                listErrors = []
                for sParameter in listMissingParameters:
                    listErrors.append("Parameter is missing: " + sParameter)
                oTestResult = {}
                oTestResult["Test"] = vTestRule
                oTestResult["ReturnCode"] = 1
                oTestResult["Messages"] = listErrors
                oOverallTestDetails.append(oTestResult)
                break;
        
            # Execute the test if all parameters are present
            else:
                try:
                    oResult = testDefinition["funct"](pData, oTest)
                    oTestResult = {}
                    oTestResult["Test"] = vTestRule
                    oTestResult["ReturnCode"] = oResult["ReturnCode"]
                    oTestResult["Messages"] = oResult["Messages"]
                    oOverallTestDetails.append(oTestResult)
                except Exception as e:
                    iOverallReturnCode = max(1, iOverallReturnCode)
                    oTestResult = {}
                    oTestResult["Test"] = vTestRule
                    oTestResult["ReturnCode"] = 1
                    oTestResult["Messages"] = ["The test could not be run: " + str(e)]
                    oOverallTestDetails.append(oTestResult)
    
        # Report Status
        oOverallResults["TestResults"] = oOverallTestDetails
        return oOverallResults

    def validate(self, pDF, passet):
        """Iterate through validation rules and apply them."""
        listTestsToExecute = []
        sBusinessDate = self.pAssetGroupConfig['BusinessDate'].replace("-","/") + "/"
        fileName=f"/Volumes/{self.pEnvConfig['Catalog_Name']}/db_fnd_ing/vol_ingest_fnd/{passet['Folder_Path_Name']}{sBusinessDate}{passet['Ingest_File_Pattern_String']}"
        # Combine passet and its Fields into a single iterable
        lPasset = chain([passet], passet['Fields'])

        try:
            # Iterate over the combined iterable
            for item in lPasset:
                if 'Validation_Rules_String' in item and item['Validation_Rules_String']:
                    rules = json.loads(item['Validation_Rules_String'])
                    for rule in rules:
                        rule_type = rule['RuleType']
                        testToExecute = {'TestType': rule_type}
                        if rule_type in ['MinRows', 'MaxRows', 'AllowDuplicates', 'AllowBlankPk']:
                            testToExecute['Value'] = rule['Value']
                        elif rule_type in ['Minimum', 'Maximum', 'AllowedValues']:
                            testToExecute['Field'] = item['Target_Field_Name']
                            if rule_type == 'AllowedValues':
                                testToExecute['Values'] = rule['Values']
                            else:
                                testToExecute['Value'] = rule['Value']
                        elif rule_type == 'NoNulls':
                            testToExecute['FieldList'] = item['Target_Field_Name']
                        testToExecute['Severity'] = rule['Severity']
                        listTestsToExecute.append(testToExecute)
            oResults = self.executeTests(pDF, listTestsToExecute,passet)

            # Extract the test results
            results = oResults['TestResults']
            
            

            # Determine test case status based on ReturnCode
            testCaseStatus = "Passed" if all(result['ReturnCode'] != 1 for result in results) else "Failed"

            # Append the status to the results
            oResults['TestResults'].append({'FileName': fileName})
            oResults['TestResults'].append({'Status': testCaseStatus})
            return oResults

        except Exception as e:
            # Handle the exception and return to main class
            listErrors = []
            oOverallTestDetails=[]
            listErrors.append(f"Parameter is missing: {e}")
            oTestResult = {}
            oTestResult["TestResults"] = item
            oTestResult["ReturnCode"] = 1
            oTestResult["Messages"] = listErrors
            oOverallTestDetails.append(oTestResult)
            return oOverallTestDetails

    