from pyspark.sql.functions import col, to_date, date_format, upper, lower, regexp_replace, substring, udf
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
import datetime

class DIFFieldConversion:
    def __init__(self, aEnvConfig, aLogger, aSpark, pAsset):
        self.aLogger = aLogger
        self.aSpark = aSpark
        self.aAsset = pAsset

    def convertStringToDate(self, df, field, fmt):
        return df.withColumn(field, to_date(col(field), fmt))

    def convertDateToString(self, df, field, fmt):
        return df.withColumn(field, date_format(col(field), fmt))

    def convertStringToString(self, df, field, fmt):
        return df.withColumn(field, date_format(to_date(col(field), 'yyyy-MM-dd'), fmt))

    def convertToUpper(self, df, field):
        return df.withColumn(field, upper(col(field)))

    def convertToLower(self, df, field):
        return df.withColumn(field, lower(col(field)))

    def convertDecimalCommaToFloat(self, df, field):
        return df.withColumn(field, regexp_replace(col(field), ",", ".").cast(FloatType()))

    def convertCurrencyDollarToFloat(self, df, field):
        return df.withColumn(field, regexp_replace(regexp_replace(col(field), "[$]", ""), ",", "").cast(FloatType()))

    def convertDateToInt(self, df, field):
        def date_to_days(d):
            if d:
                base = datetime.date(1900, 1, 1)
                if isinstance(d, datetime.datetime):
                    d = d.date()
                return (d - base).days if isinstance(d, datetime.date) else None
            return None
        return df.withColumn(field, udf(date_to_days, IntegerType())(col(field)))

    def convertStringToInt(self, df, field):
        return df.withColumn(field, col(field).cast(IntegerType()))

    def convertStringToFloat(self, df, field):
        return df.withColumn(field, col(field).cast(FloatType()))

    def convertIntToFloat(self, df, field):
        return df.withColumn(field, col(field).cast(FloatType()))

    def convertStandard(self, df, field, sourceType, targetType):
        if sourceType == "String" and targetType == "Int":
            return self.convertStringToInt(df, field)
        elif sourceType == "String" and targetType == "Float":
            return self.convertStringToFloat(df, field)
        elif sourceType == "Int" and targetType == "Float":
            return self.convertIntToFloat(df, field)
        elif sourceType == "Date" and targetType == "Int":
            return self.convertDateToInt(df, field)
        return df

    def applyConversions(self, pDF):
        df = pDF
        self.aLogger.info("Starting field conversion...")

        for dField in self.aAsset["Fields"]:
            if dField.get("Exclude_From_Processed_Indicator") == "Y":
                continue

            field = dField["Target_Field_Name"]
            sourceType = dField["Source_Data_Type_Code"]
            targetType = dField["Target_Data_Type_Code"]
            ruleString = dField.get("Conversion_Rule_String", "Standard")
            ruleList = [r.strip() for r in ruleString.split(",")]

            self.aLogger.info(f"\n Field: {field} | Source: {sourceType} to  Target: {targetType} | Rules: {ruleList}")

            try:
                for rule in ruleList:
                    if rule.startswith("Date:"):
                        fmt = rule.split("Date:")[1]
                        if sourceType == "String" and targetType == "Date":
                            df = self.convertStringToDate(df, field, fmt)
                        elif sourceType == "Date" and targetType == "String":
                            df = self.convertDateToString(df, field, fmt)
                        elif sourceType == "String" and targetType == "String":
                            df = self.convertStringToString(df, field, fmt)
                        else:
                            raise ValueError(f"Invalid Date conversion: {sourceType} to {targetType}")
                        self.aLogger.info(f"Applied: {rule}")

                    elif rule.startswith("Left:"):
                        n = int(rule.split(":")[1])
                        df = df.withColumn(field, substring(col(field), 1, n))
                        self.aLogger.info(f"Applied: {rule}")

                    elif rule == "Upper":
                        df = self.convertToUpper(df, field)
                        self.aLogger.info(f"Applied: {rule}")

                    elif rule == "Lower":
                        df = self.convertToLower(df, field)
                        self.aLogger.info(f"Applied: {rule}")

                    elif rule == "Decimal:Comma":
                        df = self.convertDecimalCommaToFloat(df, field)
                        self.aLogger.info(f"Applied: {rule}")

                    elif rule == "Currency:Dollar":
                        df = self.convertCurrencyDollarToFloat(df, field)
                        self.aLogger.info(f"Applied: {rule}")

                    elif rule == "Standard":
                        df = self.convertStandard(df, field, sourceType, targetType)
                        self.aLogger.info(f"Applied: {rule}")

                self.aLogger.info(f"Transformed: {field}")
            except Exception as e:
                self.aLogger.error(f"Error - Field: {field} | Reason: {str(e)}")

        self.aLogger.info("Field conversion complete.")
        return df