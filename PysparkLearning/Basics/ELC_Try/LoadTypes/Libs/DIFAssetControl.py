from croniter import croniter
from datetime import datetime, timedelta, date
import json
import re
import pyodbc
import calendar

from DatabaseConstants import TableNames, ProjectColumns, AssetGroupColumns, AssetColumns, FieldColumns, IncrementalLoadColumns, AssetHistoryColumns, AssetStatusColumns, HistoricalLoadColumns, HolidayCalendarColumns

class DIFAssetControl:
    def __init__(self, aEnvConfig, aLogger, sparkSession, pAsset):
        """Initialize Asset Control by calculating NextExpectedDate and checking Asset Status."""
        self.aEnvConfig = aEnvConfig
        self.aLogger = aLogger
        self.sparkSession = sparkSession
        self.aAsset = pAsset
        self.assetStatus = self.aAsset.get("Current_Asset_Status_Code", "Inactive")

        try:
            earlyLateRuleStr = self.aAsset.get("Early_Late_File_Rule_String", "{}")  # Get JSON string or empty dict
            self.earlyLateChecks = json.loads(earlyLateRuleStr)  # Convert JSON string to dict
        except json.JSONDecodeError as e:
            self.aLogger.error(f"Early_Late_File_Rule_String Unavailable or : {e}")
            self.earlyLateChecks = {}

        self.MultipleFilesFailureAction = self.aAsset.get(f'{AssetColumns.Multiple_Files_Failure_Action_Code}', None)
        self.EaryLateFailureAction = self.earlyLateChecks.get("EarlyLateFailAction", None)
        self.skipHoliday = self.earlyLateChecks.get("SkipHoliday", True)
        self.skipWeekend = self.earlyLateChecks.get("SkipWeekend", True)

        self.scheduleType = self.earlyLateChecks.get("ScheduleType", None)
        self.schedule = self.earlyLateChecks.get("Schedule", None)

        self.bufferEarly = self.earlyLateChecks.get("BufferEarly", 0)
        self.bufferLate = self.earlyLateChecks.get("BufferLate", 0)

        self.holidayDates = None
        self.HolidayCalendars = self.earlyLateChecks.get("Calenders", [])
        if not isinstance(self.HolidayCalendars, list):
            self.HolidayCalendars = [self.HolidayCalendars]


    def validateFileSequence(self, fileName):
        """
        Validates a file by checking sequence order and expected arrival time.

        Args:
            fileName (str): Name of the file to validate.

        Returns:
            tuple: (bool, str) - True with a reason if validation fails, otherwise (False, None).
        """
        lastCompletedFile = self.__getLastCompletedFileName()

        # Check if the file is out of sequence
        outOfSequence, reason = self.__isOutOfSequence(fileName, lastCompletedFile)
        if outOfSequence:
            return True, reason

        # Check if the file is early or late
        if self.earlyLateChecks != {}:
            earlyLate, reason = self.__earlyLateCheck(fileName, lastCompletedFile)
        else:
            earlyLate, reason = False, None

        if earlyLate:
            return True, reason

        return False, None  # File is valid


    def __isOutOfSequence(self, fileName, sLastFileName):
        """
        Determines if the given file is out of sequence based on the last processed file.

        Args:
            fileName (str): Current file name.
            sLastFileName (str): Last successfully processed file name.

        Returns:
            tuple: (bool, str) - True with "OutOfSequence" if the file is out of order, otherwise (False, None).
        """
        try:
            self.aLogger.debug("isOutOfSequence.Start")

            Multiple_Files_Sequence_Check_Indicator = self.aAsset.get(f'{AssetColumns.Multiple_Files_Sequence_Check_Indicator}', "Y")
            # Skip sequence check if not required
            if Multiple_Files_Sequence_Check_Indicator == "N":
                self.aLogger.debug("Sequence check not needed")
                return False, None

            self.aLogger.debug(f"LastFile: {sLastFileName}")
            self.aLogger.debug(f"ThisFile: {fileName}")

            # If no previous file exists, sequence validation is not applicable
            if sLastFileName is None:
                self.aLogger.debug("Last date is None ... never checked")
                return False, None

            if sLastFileName > fileName:
                self.aLogger.debug("Out Of Sequence is TRUE")
                return True, "OutOfSequence"

            self.aLogger.debug("Out Of Sequence is FALSE")
            self.aLogger.debug("isOutOfSequence.End.OK")
            return False, None

        except Exception as ex:
            self.aLogger.error("isOutOfSequence.End.Error: " + str(ex))
            raise Exception(str(ex))


    def __earlyLateCheck(self, fileName, lastCompletedFile):
        """
        Determines if a file is early, late, or on time by calculating the expected date 
        and applying buffer, weekend, and holiday adjustments.

        Args:
            fileName (str): Name of the file to validate.
            lastCompletedFile (str): Last successfully processed file name.

        Returns:
            tuple: (bool, str) - True with "Early", "Late", or "INVALID_FILE_NAME" if validation fails, otherwise (False, None).
        """
        try:
            self.aLogger.debug("earlyLateCheck.Start")
            # Retrieve the last processed file date
            pLastDate = self.__getpLastDate(lastCompletedFile)

            # Calculate the next expected date based on scheduling rules
            nextExpectedDate = self.__calculateNextExpectedDate(pLastDate)
            self.aLogger.info(f"NextExpectedDate: {nextExpectedDate}")

            # Extract the business date from the file name
            fileDate = self.__extractDateFromFileName(fileName)
            if not fileDate:
                self.aLogger.info(f"Skipping file {fileName}: Could not extract business date.")
                return True, "INVALID_FILE_NAME"

            # Compute the valid buffer range
            nextExpectedDateMin = nextExpectedDate - timedelta(days=self.bufferEarly)
            nextExpectedDateMax = nextExpectedDate + timedelta(days=self.bufferLate)

            # Check if the file date falls within the allowed buffer range
            if nextExpectedDateMin <= fileDate <= nextExpectedDateMax:
                return False, None  # File is within the acceptable range

            # Adjust the date difference by considering skipped weekends and holidays
            adjustedDelta = self.__adjustDeltaForSkippedDays(nextExpectedDate, fileDate)

            # Re-evaluate file validity with adjusted delta
            if -self.bufferEarly <= adjustedDelta <= self.bufferLate:
                return False, None  # File is within the adjusted buffer range

            self.aLogger.debug("earlyLateCheck.End.OK")
            return True, "Early" if adjustedDelta < 0 else "Late"

        except Exception as ex:
            self.aLogger.error(f"earlyLateCheck.End.Error: {ex}")
            raise Exception(str(ex))


    def __adjustDeltaForSkippedDays(self, expectedDate, actualDate):
        """
        Adjusts the date difference (delta) by accounting for weekends and holidays 
        between the expected and actual file dates.

        Args:
            expectedDate (date): The expected file arrival date.
            actualDate (date): The actual file arrival date.

        Returns:
            int: Adjusted delta after excluding weekends and holidays.
        """
        try:
            # Calculate the raw difference in days between actual and expected dates
            delta = (actualDate - expectedDate).days
            self.aLogger.info(f"Initial delta: {delta}")

            if delta == 0:
                return 0  # No adjustment needed if dates are the same

            skippedDays = 0
            startDate, endDate = min(expectedDate, actualDate), max(expectedDate, actualDate)

            # Fetch holiday dates if not already loaded
            if self.holidayDates is None:
                self.holidayDates = self.__getHolidays(self.HolidayCalendars)

            # Iterate through each day in the range to adjust for weekends and holidays
            for day in range((endDate - startDate).days):
                currentDate = startDate + timedelta(days=day)
                self.aLogger.info(f"Checking date: {currentDate}")

                # Skip weekends and holidays if configured
                if (self.skipWeekend and currentDate.weekday() >= 5) or (
                    self.skipHoliday and currentDate in self.holidayDates):
                    
                    self.aLogger.info(f"Skipping date: {currentDate}")
                    skippedDays += 1
                    
                    # Adjust delta accordingly
                    delta = delta - 1 if delta > 0 else delta + 1 

                    # Stop early if delta reaches zero
                    if delta == 0:
                        self.aLogger.info(f"Breaking early: Adjusted delta reached zero on {currentDate} after skipping {skippedDays} days.")
                        break

            self.aLogger.info(f"Final adjusted delta: {delta}, Skipped days: {skippedDays}")
            return delta

        except Exception as ex:
            self.aLogger.error(f"adjustDeltaForSkippedDays.End.Error: {ex}")
            raise Exception(str(ex))


    def __calculateNextExpectedDate(self, pLastDate):
        """
        Determines the next expected business date based on the scheduling type.

        Args:
            pLastDate (date): The last processed file's business date.

        Returns:
            date: The next expected business date.
        """
        # If no last date is available, use today's date as default next expected date.
        if pLastDate is None:
            nextDate = datetime.today().date()
            return nextDate

        # Default to the next day if no specific schedule is defined
        if self.scheduleType is None:
            nextDate = pLastDate + timedelta(days=1)
        elif self.scheduleType.lower() == "cron":
            nextDate = self.__getNextExpectedDateCron(pLastDate)
        elif self.scheduleType.lower() == "dom":
            nextDate = self.__getNextExpectedDateDom(pLastDate)

        return nextDate

    def __getNextExpectedDateCron(self, pLastDate):
        """Calculates the next expected date using a Cron expression."""
        try:
            self.aLogger.debug("getNextExpectedDate_Cron: " + self.schedule)

            # Ensure pLastDate is a datetime object
            if isinstance(pLastDate, date):
                pLastDate = datetime.combine(pLastDate, datetime.min.time()) 

            # Validate the Cron expression before proceeding
            if not croniter.is_valid(self.schedule):
                raise Exception("Invalid CRON expression: " + self.schedule)

            # Initialize croniter with the provided schedule and last processed date
            cron = croniter(self.schedule, pLastDate)
            dNext = cron.get_next(datetime)  # Get the next scheduled date

            # Ensure the next date is not the same as the last processed date
            if dNext.date() == pLastDate.date():
                dNext = cron.get_next(datetime)  # Fetch the next occurrence

            self.aLogger.debug("CRON Next expected date is: " + str(dNext.date()))
            return dNext.date()  # Return the calculated next expected date
            
        except Exception as ex:
            # Log the error and raise an exception with details
            self.aLogger.error(f"Error calculating next expected date for asset {self.aAsset[f'{AssetColumns.Asset_Name}']}: {str(ex)}")
            raise

    def __getNextExpectedDateDom(self, pLastDate):
        """Returns the Nth occurrence of a weekday (Mon=0, Tue=1, ..., Sun=6) in the next month."""
        try:
            self.aLogger.debug(f"getNextExpectedDateDom: {self.schedule}")

            # Parse the schedule to get the desired week number and day of the week
            week, dayOfWeek = map(int, self.schedule.split(","))
            
            # Move to the next month
            year, month = pLastDate.year, pLastDate.month + 1
            if month > 12:
                month, year = 1, year + 1  # Adjust for year rollover
            
            # Determine the first day of the new month
            firstDay = datetime(year, month, 1)
            
            # Find the first occurrence of the specified weekday in the month
            firstOccurrence = firstDay + timedelta(days=((dayOfWeek - firstDay.weekday()) + 7) % 7)
            
            # Compute the Nth occurrence of the weekday in the month
            nthOccurrence = firstOccurrence + timedelta(days=7 * (week - 1))
            
            return nthOccurrence.date()  # Return the computed date

        except Exception as ex:
            # Log and raise an error if date calculation fails
            self.aLogger.error(f"Error calculating next expected DOM date for asset {self.aAsset[f'{AssetColumns.Asset_Name}']}: {str(ex)}")
            raise

    def __getHolidays(self, assetCalendars):
        """Fetch holiday dates for the asset's regions from the holiday table."""
        try:
            if not assetCalendars:
                return set()

            calendarList = ", ".join(f"'{cal}'" for cal in assetCalendars)

            sSQL = f"""
                SELECT DISTINCT {HolidayCalendarColumns.Holiday_Date} 
                FROM {self.aEnvConfig['MetaData_Schema']}.{TableNames.Holiday_Calendar} 
                WHERE {HolidayCalendarColumns.Calendar_Name} IN ({calendarList})
            """

            self.aLogger.info(f"Executing SQL for holidays: {sSQL}")

            with self.__getSQLConnection() as conn:
                cursor = conn.cursor()
                cursor.execute(sSQL)
                holidayDates = {row[0] for row in cursor.fetchall()}

            self.aLogger.info(f"Holiday dates for Calendars {assetCalendars}: {holidayDates}")
            return holidayDates

        except Exception as ex:
            self.aLogger.error(f"Error fetching holidays for regions {assetCalendars}: {str(ex)}")
            return set()

    def __getLastCompletedFileName(self):

        G_SQLSelectMaxFileForStep = f"""Select Max({AssetHistoryColumns.File_Name}) as MaxFileName from <DIFSCHEMA>.{TableNames.AssetHistory}  
        where {AssetHistoryColumns.Asset_Key}=<AssetID_FK>  and {AssetHistoryColumns.Step_Name}='<Step>' and {AssetHistoryColumns.Status_Code}='OK'"""

        sSQL = G_SQLSelectMaxFileForStep
        sSQL = sSQL.replace("<DIFSCHEMA>", self.aEnvConfig["MetaData_Schema"])
        sSQL = sSQL.replace("<AssetID_FK>", str(self.aAsset[f'{AssetColumns.Asset_Key}']))
        sSQL = sSQL.replace("<Step>", 'IngestToRaw.End')

        with self.__getSQLConnection() as oConn:
            with oConn.cursor() as oCur:
                self.aLogger.debug(sSQL)
                oCur.execute(sSQL)
                row = oCur.fetchone()
                lastCompletedFile = row[0] if row and row[0] else None

        return lastCompletedFile

    def __getpLastDate(self, lastCompletedFile):
        """Fetch the last ingested file date from the Asset History table using SQL."""
        try:
            return self.__extractDateFromFileName(lastCompletedFile) if lastCompletedFile else None
        
        except Exception as ex:
            self.aLogger.error(f"Error fetching last completed date for Asset {lastCompletedFile}: {str(ex)}")


    def __extractDateFromFileName(self, fileName):
        """Extracts date from the file path (expected format: DIFTest/Group01/YYYY/MM/DD/Students.csv).""" 
        try: 
            match = re.search(r'(\d{4})/(\d{2})/(\d{2})', fileName) 
            if match: 
                year, month, day = map(int, match.groups()) 
                extractedDate = datetime(year, month, day).date()
                self.aLogger.info(f"BusinessDate extracted from file: {str(extractedDate)}")
                return extractedDate
            else: 
                self.aLogger.error(f"Could not extract date from filename: {fileName}") 
                return None 
        except Exception as ex: 
            self.aLogger.error(f"Error extracting date from filename {fileName}: {str(ex)}") 
            return None 

    def __getSQLConnection(self):
        """Establishes SQL Connection."""
        self.aLogger.debug("getSQLConnection.Start")
        try:
            conn = pyodbc.connect(self.aEnvConfig["SQLConnString"])
            return conn
        except Exception as ex:
            self.aLogger.error("getSQLConnection.Error: " + str(ex))
            raise Exception(str(ex))