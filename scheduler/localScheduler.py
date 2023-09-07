import subprocess
import time
from multiprocessing import Process
import psutil
from scheduler import Scheduler


# ANSI escape codes for text color formatting
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'



class LocalScheduler(Scheduler):

    def runProgram(self):
        programCommandStatus = subprocess.run(self.programCommand, shell=True,capture_output=True, encoding="utf-8")
        self.exitCode = programCommandStatus.returncode
        if programCommandStatus.returncode == 0:
            self.jobStatus = "COMPLETED"
        elif self.jobStatus != "KILLED":
            self.jobStatus = "FAILED"
        exit(programCommandStatus.returncode)
        #if programCommandStatus.returncode != 0 :
         #   self.printError("Error ruuning the program command: " + programCommandStatus.stderr)
          #  exit(1)
    def launchJob(self):
        self.logger.info("Launching + " + self.programName + "...")
        self.process = Process(target=self.runProgram)
        self.process.start()
        self.jobId = self.process.pid
        #self.process = subprocess.Popen(self.programCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True, encoding="utf-8")

        self.logger.info("Launched " + self.programName + " process with process id:" + str(self.process.pid))

    def isJobAlive(self):
        if self.process and self.process.is_alive():
            return True
        else:
            return False

    def getJobStatus(self):
        if self.process and self.process.is_alive():
            return "RUNNING"
        else:
            return self.jobStatus

    def getJobExitCode(self):
          return self.exitCode

    def getJobId(self):
        return self.jobId

    def killJob(self):
        if self.process and self.process.is_alive():
            self._terminateProcessAndChildren(self.process.pid)
            self.logger.info(self.programName + " job and all its child processes terminated")
        else:
            self.logger.error("No " + self.programName + " job running to terminate")

    def _terminateProcessAndChildren(self, pid):
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