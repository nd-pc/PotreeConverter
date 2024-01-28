from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

from ..loggingwrapper import LoggingWrapper

class Scheduler:
    """Abstract class for job schedulers"""
    def __init__(self, programCommand, programName):
        self.programCommand = programCommand
        self.programName = programName
        self.process = None
        self.jobId = None
        self.jobStatus = None
        self.exitCode = None


    def launchJob(self):
        raise NotImplementedError("Subclasses must implement this method")

    def isJobAlive(self):
        raise NotImplementedError("Subclasses must implement this method")

    def getjobStatus(self):
        raise NotImplementedError("Subclasses must implement this method")
    def getJobId(self):
        raise NotImplementedError("Subclasses must implement this method")
    def getJobExitCode(self):
        raise NotImplementedError("Subclasses must implement this method")
    def killJob(self):
        raise NotImplementedError("Subclasses must implement this method")


