import subprocess
import time
from datetime import datetime
import shutil
from multiprocessing import Process

from .scheduler import Scheduler

from ..loggingwrapper import LoggingWrapper




class SbatchScheduler(Scheduler):
    """Class for SLURM job scheduler"""

    def launchJob(self):
        """Launches the job"""
        LoggingWrapper.info(f"Submitting {self.programName} sbatch job...")
        process = subprocess.run(self.programCommand, shell=True,  capture_output=True, encoding="utf-8")
        if process.returncode != 0:
            print(process.stderr)
            LoggingWrapper.error("Something went wrong in submitting the job")
            exit(1)

        output = process.stdout
        self.jobId = output.split()[-1]
        LoggingWrapper.info(f"Submitted {self.programName} sbatch job with job id:" + str(self.jobId))

        self.process = Process(target=self.monitorJob)
        self.process.start()



    def monitorJob(self):
        """Monitors the job status"""
        ST= "PENDING"  # Status to be checked
        self.jobStatus = ST
        LoggingWrapper.info(f"Waiting for {self.programName} sbatch job to start...", color="yellow")
        n = 0
        while not ST.startswith("RUNNING"):
            if n == 10:
                LoggingWrapper.info(f"Waiting for {self.programName} sbatch job to start...", color="yellow")
                n = 0
            n += 1
            cmd = shutil.which("sacct") + " -j " + self.jobId + " -o State"
            process = subprocess.run(cmd, shell=True,  capture_output=True, encoding="utf-8")
            if process.returncode != 0:
                LoggingWrapper.error("Something went wrong in checking the job status")
                exit(1)
            output = process.stdout
            ST = output.split()[-1]

            if ST.startswith("CANCELLED"):
                LoggingWrapper.info(f"{self.programName} sbatch job cancelled", color= "red")  # Show humans some info if the job is cancelled
                self.jobStatus = "KILLED"
                exit(1)
            time.sleep(15)

        self.jobStatus = "RUNNING"
        LoggingWrapper.info(f"{self.programName} sbatch job started at " + str(datetime.now()), color="green")
        # Monitoring loop
        while not ST.startswith("COMPLETED"):
            cmd = shutil.which("sacct") + " -j " + self.jobId + " -o State"
            process = subprocess.run(cmd, shell=True,  capture_output=True, encoding="utf-8")
            if process.returncode != 0:
                LoggingWrapper.error("Something went wrong in checking the job status")
                exit(1)
            output = process.stdout
            if len(output.split()) < 3:
                LoggingWrapper.error(f"{self.programName} sbatch job not found")
                exit(1)
            ST = output.split()[2]


            time.sleep(15)  # Time interval between checks

            if ST.startswith("FAILED"):
                LoggingWrapper.error(f"{self.programName} sbatch job failed")  # Show humans some info if the job fails
                self.jobStatus = "FAILED"
                exit(1)
            elif ST.startswith("CANCELLED"):
                LoggingWrapper.error(f"{self.programName} sbatch job cancelled by the user")  # Show humans some info if the job is cancelled
                self.jobStatus = "KILLED"
                exit(1)
            elif ST.startswith("TIMEOUT"):
                LoggingWrapper.error(f"{self.programName} sbatch job timeout")
                self.jobStatus = "TIMEOUT"
                exit(1)

        LoggingWrapper.info(f"{self.programName} sbatch job finished successfully", color="green")  # Show humans some info when the job finishes
        self.jobStatus = "COMPLETED"
        exit(0)

    def isJobAlive(self):
        """Checks if the job is alive"""
        if self.process and self.process.is_alive():
            return True
        else:
            return False

    def getJobStatus(self):
        """Returns the job status"""
        return self.jobStatus

    def getJobExitCode(self):
        """Returns the job exit code"""
        if self.process and not self.process.is_alive():
            self.exitCode = self.process.exitcode
            return self.exitCode
        else:
            return None

    def getJobId(self):
        """Returns the job id"""
        return self.jobId


    def killJob(self):
        """Kills the job"""
        if self.jobId:
            cmd = shutil.which("scancel") + " " + self.jobId
            subprocess.run(cmd, shell=True)
            LoggingWrapper.info(f"{self.programName} sbatch job with job id {self.jobId} has been cancelled", color="red")
        else:
            LoggingWrapper.error(f"No {self.programName} sbatch job running to cancel")