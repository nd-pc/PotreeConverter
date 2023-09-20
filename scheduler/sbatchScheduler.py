import subprocess
import time
from datetime import datetime
import shutil
from multiprocessing import Process

from scheduler import Scheduler



# ANSI escape codes for text color formatting
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'



class SbatchScheduler(Scheduler):

    def launchJob(self):
        self.logger.info(f"Submitting {self.programName} sbatch job...")
        process = subprocess.run(self.programCommand, shell=True,  capture_output=True, encoding="utf-8")
        if process.returncode != 0:
            print(self.programCommand)
            self.printError("Something went wrong in submitting the job")
            exit(1)


        output = process.stdout
        self.jobId = output.split()[-1]
        self.logger.info(f"Submitted {self.programName} sbatch job with job id:" + str(self.jobId))

        self.process = Process(target=self.monitorJob)
        self.process.start()


    def monitorJob(self):
        ST= "PENDING"  # Status to be checked
        self.jobStatus = ST
        self.logger.info(f"Waiting for {self.programName} sbatch job to start...", color="yellow")
        n = 0
        while not ST.startswith("RUNNING"):
            if n == 10:
                self.logger.info(f"Waiting for {self.programName} sbatch job to start...", color="yellow")
                n = 0
            n += 1
            cmd = shutil.which("sacct") + " -j " + self.jobId + " -o State"
            process = subprocess.run(cmd, shell=True,  capture_output=True, encoding="utf-8")
            if process.returncode != 0:
                self.logger.error("Something went wrong in checking the job status")
                exit(1)
            output = process.stdout
            ST = output.split()[-1]
            if ST.startswith("CANCELLED"):
                self.logger.info(f"{self.programName} sbatch job cancelled", color= "red")  # Show humans some info if the job is cancelled
                self.jobStatus = "KILLED"
                exit(1)
            time.sleep(15)

        self.jobStatus = "RUNNING"
        self.logger.info(f"{self.programName} sbatch job started at " + str(datetime.now()), color="green")
        # Monitoring loop
        while not ST.startswith("COMPLETED"):
            cmd = shutil.which("sacct") + " -j " + self.jobId + " -o State"
            process = subprocess.run(cmd, shell=True,  capture_output=True, encoding="utf-8")
            if process.returncode != 0:
                self.logger.error("Something went wrong in checking the job status")
                exit(1)
            output = process.stdout
            ST = output.split()[-1]

            time.sleep(15)  # Time interval between checks

            if ST.startswith("FAILED"):
                self.logger.error(f"{self.programName} sbatch job failed")  # Show humans some info if the job fails
                self.jobStatus = "FAILED"
                exit(1)
            elif ST.startswith("CANCELLED"):
                self.logger.info(f"{self.programName} sbatch job cancelled", color="red")  # Show humans some info if the job is cancelled
                self.jobStatus = "KILLED"
                exit(1)
            elif ST.startswith("TIMEOUT"):
                self.logger.error(f"{self.programName} sbatch job timeout")
                self.jobStatus = "TIMEOUT"
                exit(1)

        self.logger.info(f"{self.programName} sbatch job finished successfully", color="green")  # Show humans some info when the job finishes
        self.jobStatus = "COMPLETED"
        exit(0)

    def isJobAlive(self):
        if self.process and self.process.is_alive():
            return True
        else:
            return False

    def getJobStatus(self):
       return self.jobStatus

    def getJobExitCode(self):
        if self.process and not self.process.is_alive():
            self.exitCode = self.process.exitcode
            return self.exitCode
        else:
            return None

    def getJobId(self):
        return self.jobId

    def killJob(self):
        if self.jobId:
            cmd = shutil.which("scancel") + " " + self.jobId
            subprocess.run(cmd, shell=True)
            self.logger.info(f"{self.programName} sbatch job with job id {self.jobId} has been cancelled", colorama="red")
        else:
            self.logger.error(f"No {self.programName} sbatch job running to cancel")