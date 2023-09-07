import subprocess
import time
from datetime import datetime
import shutil
from multiprocessing import Process
from scheduler import Scheduler

class QsubScheduler(Scheduler):

    def launchJob(self):
        self.logger.info(f"Submitting {self.programName} qsub job...")
        process = subprocess.run(self.programCommand, shell=True, capture_output=True, encoding="utf-8")
        if process.returncode != 0:
            self.logger.error("Something went wrong in submitting the job")
            exit(1)

        output = process.stdout
        job_info = output.split('.')

        if len(job_info) < 1:
            self.logger.error("Failed to retrieve job ID")
            exit(1)

        self.jobId = job_info[0]
        self.logger.info(f"Submitted {self.programName} qsub job with job id: {self.jobId}")

        self.process = Process(target=self.monitorJob)
        self.process.start()

    def monitorJob(self):
        ST = "Q"  # Status to be checked (Queued)
        self.jobStatus = ST
        self.logger.info(f"Waiting for {self.programName} qsub job to start...")
        n = 0
        while not ST.startswith("R"):
            if n == 10:
                self.logger.info(f"Waiting for {self.programName} qsub job to start...")
                n = 0
            n += 1
            cmd = shutil.which("qstat") + " -x " + self.jobId
            process = subprocess.run(cmd, shell=True, capture_output=True, encoding="utf-8")
            if process.returncode != 0:
                self.logger.error("Something went wrong in checking the job status")
                exit(1)
            output = process.stdout
            job_info = output.splitlines()
            if len(job_info) < 3:
                self.logger.error(f"{self.programName} qsub job not found")
                exit(1)
            ST = job_info[2].split()[4]

            if ST.startswith("C"):
                self.logger.info(f"{self.programName} qsub job cancelled", color="red")  # Show humans some info if the job is cancelled
                self.jobStatus = "KILLED"
                exit(1)
            time.sleep(15)

        self.jobStatus = "RUNNING"
        self.logger.info(f"{self.programName} qsub job started at " + str(datetime.now()))
        # Monitoring loop
        while not ST.startswith("C"):
            cmd = shutil.which("qstat") + " -x " + self.jobId
            process = subprocess.run(cmd, shell=True, capture_output=True, encoding="utf-8")
            if process.returncode != 0:
                self.logger.error("Something went wrong in checking the job status")
                exit(1)
            output = process.stdout
            job_info = output.splitlines()
            if len(job_info) < 3:
                self.logger.error(f"{self.programName} qsub job not found")
                exit(1)
            ST = job_info[2].split()[4]

            time.sleep(15)  # Time interval between checks

            if ST.startswith("F"):
                self.logger.error(f"{self.programName} qsub job failed")  # Show humans some info if the job fails
                self.jobStatus = "FAILED"
                exit(1)
            elif ST.startswith("C"):
                self.logger.info(f"{self.programName} qsub job cancelled", color="red")  # Show humans some info if the job is cancelled
                self.jobStatus = "KILLED"
                exit(1)
            elif ST.startswith("T"):
                self.logger.error(f"{self.programName} qsub job timeout")
                self.jobStatus = "TIMEOUT"
                exit(1)

        self.logger.info(f"{self.programName} qsub job finished successfully", color="green")  # Show humans some info when the job finishes
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
            cmd = shutil.which("qdel") + " " + self.jobId
            subprocess.run(cmd, shell=True)
            self.logger.info(f"{self.programName} qsub job with job id {self.jobId} has been cancelled", colorama="red")
        else:
            self.logger.error(f"No {self.programName} qsub job running to cancel")
