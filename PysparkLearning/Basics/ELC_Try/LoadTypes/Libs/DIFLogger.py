import DP_Utility
from DP_Utility import *
import traceback,sys
    
class DIFLogger():   
    def __init__(self,pFocusArea,pApplication,pConsoleLevel,pLogLevel):
        self.aFocusArea = pFocusArea
        self.aApplication = pApplication
        self.aConsoleLevel = pConsoleLevel
        self.aLogLevel = pLogLevel
        self.aLogger = DP_Logger(self.aFocusArea,self.aApplication, self.aConsoleLevel, self.aLogLevel)

    def debug(self,pMessage):
        self.aLogger.debug(pMessage)

    def info(self,pMessage):
        self.aLogger.info(pMessage)
    
    def warn(self,pMessage):
        self.aLogger.warn(pMessage)

    def error(self,pMessage):
        sText = pMessage
        try:
            lStack = traceback.extract_stack()
            sText = pMessage + str(lStack[-1])
        except:
            pass
        self.aLogger.error(pMessage)

    def __getattr__(self, name):
        """
        Delegate missing attributes/methods to underlying DP_Logger instance
        so DIFLogger acts like a drop-in replacement.
        """
        return getattr(self.aLogger, name)

        
        
