import subprocess
from multiprocessing import Process
import psutil
from scheduler import Scheduler

from ..loggingwrapper import LoggingWrapper




class LocalScheduler(Scheduler):
    """Class for local job scheduler"""
    def runProgram(self):
        programCommandStatus = subprocess.run(self.programCommand, shell=True,capture_output=True, encoding="utf-8")
        self.exitCode = programCommandStatus.returncode
        if programCommandStatus.returncode == 0:
            self.jobStatus = "COMPLETED"
        elif self.jobStatus != "KILLED":
            self.jobStatus = "FAILED"
        exit(programCommandStatus.returncode)
    def launchJob(self):
        """Launches the job"""
        LoggingWrapper.info("Launching + " + self.programName + "...")
        self.process = Process(target=self.runProgram)
        self.process.start()
        self.jobId = self.process.pid
        #self.process = subprocess.Popen(self.programCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True, encoding="utf-8")

        LoggingWrapper.info("Launched " + self.programName + " process with process id:" + str(self.process.pid))


    def isJobAlive(self):
        """Checks if the job is alive
        :return: True if the job is alive, False otherwise"""
        if self.process and self.process.is_alive():
            return True
        else:
            return False
    def getJobStatus(self):
        """Returns the job status
        :return: the job status"""
        if self.process and self.process.is_alive():
            return "RUNNING"
        else:
            return self.jobStatus

    def getJobExitCode(self):
        """Returns the job exit code
        :return: the job exit code"""
        return self.exitCode

    def getJobId(self):
        """Returns the job id
        :return: the job id"""
        return self.jobId

    def killJob(self):
        """Kills the job"""
        if self.process and self.process.is_alive():
            self._terminateProcessAndChildren(self.process.pid)
            LoggingWrapper.info(self.programName + " job and all its child processes terminated")
        else:
            LoggingWrapper.error("No " + self.programName + " job running to terminate")

    def _terminateProcessAndChildren(self, pid):
        """Terminates the process and all its children
        :param pid: the process id"""
        previousJobStatus = self.jobStatus
        try:
            self.jobStatus = "KILLED"
            process = psutil.Process(pid)
            children = process.children(recursive=True)
            for child in children:
                child.terminate()
            psutil.wait_procs(children, timeout=5)
            process.terminate()
            process.wait(timeout=5)
        except psutil.NoSuchProcess:
            self.jobStatus = previousJobStatus