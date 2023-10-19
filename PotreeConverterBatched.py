

import configparser
import glob
import sys
from datetime import datetime
from pathlib import Path
import time
import csv
import psutil
import shutil
from scheduler import SbatchScheduler
from scheduler import LocalScheduler
from scheduler import QsubScheduler
from copier import LocalCopier

from loggingwrapper import LoggingWrapper


from multiprocessing import Process




class PotreeConverterBatched:
    def __init__(self, configFile):
        self.maxTmpSpaceAvailable = 0
        self.countingBatchSize = 0
        self.lazCompressionRatio = 0
        self.tmpDir = ""
        self.tmpInputDir = ""
        self.tmpOutputDir = ""
        self.InputDir = ""
        self.OutputDir = ""
        self.lazHeadersDir = ""
        self.lazHeadersToCopy = ""
        self.partitionsCSV = ""
        self.logger = None
        self.copierType = ""
        self.countingBatchCopier = None
        self.indexingBatchCopier = None
        self.miscCopier = None
        self.logFile = ""
        self.batchesDone = {"counting": [], "indexing": []}
        self.numFilesDone = {"counting": 0, "indexing": 0}
        self.lazFilestoProcess = []
        self.programCommand = ""
        self.programName = ""
        self.logger = None

        config = configparser.ConfigParser()
        config.read(configFile)
        sections = config.sections()
        if "LogFile" not in config["DEFAULT"]:
            print("Must specify a log file in the DEFAULT section of the config file")
            exit(1)
        else:
            self.logFile = config["DEFAULT"]["LogFile"] + f"_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.log"
            self.logger = LoggingWrapper(self.logFile)
        if "INPUT_OUTPUT" not in sections:
            self.logger.error("INPUT_OUTPUT section not found in config file")
            exit(1)
        else:
            if "InputDir" not in config["INPUT_OUTPUT"]:
                self.logger.error("InputDir not found in INPUT_OUTPUT section")
                exit(1)
            else:
                self.InputDir = config["INPUT_OUTPUT"]["InputDir"]
            if "OutputDir" not in config["INPUT_OUTPUT"]:
                self.logger.error("OutputDir not found in INPUT_OUTPUT section")
                exit(1)
            else:
                self.OutputDir = config["INPUT_OUTPUT"]["OutputDir"]
            if "LazHeadersDir" not in config["INPUT_OUTPUT"]:
                self.logger.error("LazHeadersDir not found in INPUT_OUTPUT section")
                exit(1)
            else:
                self.lazHeadersToCopy = config["INPUT_OUTPUT"]["LazHeadersDir"]
            if "PartitionsCSV" not in config["INPUT_OUTPUT"]:
                self.logger.error("PartitionsCSV not found in INPUT_OUTPUT section")
                exit(1)
            else:
                self.partitionsCSV = config["INPUT_OUTPUT"]["PartitionsCSV"]


        if "TMP_STORAGE" not in sections:
            self.logger.error("TMP_STORAGE section not found in config file")
            exit(1)
        else:
                if "TmpDir" not in config["TMP_STORAGE"]:
                    self.logger.error("TmpDir not found in TMP_STORAGE section")
                    exit(1)
                else:
                    self.tmpDir = config["TMP_STORAGE"]["TmpDir"]
                    self.tmpOutputDir = self.tmpDir + "/PotreeConverterMPI_Output"
                    if "TmpInputDir" not in config["TMP_STORAGE"]:
                        self.tmpInputDir = self.tmpDir + "/PotreeConverterMPI_InputLaz"
                        self.lazHeadersDir = self.tmpInputDir + "_headers"
                    else:
                        if config["TMP_STORAGE"]["TmpInputDir"] == self.InputDir:
                            self.tmpInputDir = self.InputDir
                            self.lazHeadersDir = self.tmpDir + "/" + Path(self.InputDir).name + "_headers"
                        else:
                            self.logger.error("TmpInputDir, if specified, cannot be different from InputDir")


                if "MaxTmpSpaceAvailable" not in config["TMP_STORAGE"]:
                    self.logger.error(
                        "MaxTmpSpaceAvailable not found in LOCAL_STORAGE section")
                    exit(1)
                else:
                    self.maxTmpSpaceAvailable = eval(config["TMP_STORAGE"]["MaxTmpSpaceAvailable"])
                self.countingBatchSize = eval(config["TMP_STORAGE"]["CountingBatchSize"])
                self.lazCompressionRatio = eval(config["TMP_STORAGE"]["LazCompressionRatio"])
        if "COPIER" not in sections:
            self.copierType = config["DEFAULT"]["CopierType"]
        else:
            self.copierType = config["COPIER"]["CopierType"]

        if self.copierType == "local":
             self.countingBatchCopier = LocalCopier(self.logger)
             self.indexingBatchCopier = LocalCopier(self.logger, self.lazCompressionRatio)
             self.miscCopier = LocalCopier(self.logger)
        else:
            self.logger.error("Copier type not recognized")
            exit(1)

        self.createDirectories()

        if "SLURM_SCHEDULER" not in sections and "LOCAL_SCHEDULER" not in sections and "TORQUE_SCHEDULER" not in sections:
            self.logger.error(
                "SLURM_SCHEDULER or LOCAL_SCHEDULER or TORQUE_SCHEDULER section not found in config file")
            exit(1)
        elif ("SLURM_SCHEDULER" in sections and "LOCAL_SCHEDULER" in sections and "TORQUE_SCHEDULER" in sections) or ("SLURM_SCHEDULER" in sections and "TORQUE_SCHEDULER" in sections or "LOCAL_SCHEDULER" in sections and "TORQUE_SCHEDULER" in sections):
            self.logger.error(
                "Only one scheduler can be specified in the config file: Available schedulers: SLURM_SCHEDULER, TORQUE_SCHEDULER, LOCAL_SCHEDULER")
            exit(1)
        elif "SLURM_SCHEDULER" in sections:

            if "ProgramPath" not in config["SLURM_SCHEDULER"]:
                self.logger.error("ProgramPath not found in SLURM_SCHEDULER section")
                exit(1)
            if "ProgramName" in config["SLURM_SCHEDULER"]:
                self.programName = config["SLURM_SCHEDULER"]["ProgramName"]
            else:
                self.programName = config["SLURM_SCHEDULER"]["ProgramPath"].split("/")[-1]

            sbatchParameters = config["SLURM_SCHEDULER"]["SbatchParameters"]
            with open(self.tmpDir + "/sbatchScript.sh", "w") as sbatchScript:
                sbatchScript.write("#!/bin/sh\n\n")
                if "SbatchParameters" in config["SLURM_SCHEDULER"]:
                    sbatchScript.write("#SBATCH " + sbatchParameters + "\n\n")
                sbatchScript.write(shutil.which("mpiexec") + " " + config["SLURM_SCHEDULER"]["ProgramPath"] + " " +
                                   config["SLURM_SCHEDULER"][
                                       "ConverterOptions"] + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir)
            self.programCommand = shutil.which("sbatch") + " " + self.tmpDir + "/sbatchScript.sh"
            self.scheduler = SbatchScheduler(self.programCommand, self.programName, self.logger)
        elif "LOCAL_SCHEDULER" in sections:
            if "ProgramPath" not in config["SLURM_SCHEDULER"]:
                self.logger.error("ProgramPath not found in LOCAL_SCHEDULER section")
                exit(1)
            if "ProgramName" in config["LOCAL_SCHEDULER"]:
                self.programName = config["LOCAL_SCHEDULER"]["ProgramName"]
            else:
                self.programName = config["SLURM_SCHEDULER"]["ProgramPath"].split("/")[-1]
            self.programCommand = shutil.which("mpiexec") + " " + config["LOCAL_SCHEDULER"]["MPIEXECParameters"] + " " + \
                                  config["LOCAL_SCHEDULER"]["ProgramPath"] + " " + config["LOCAL_SCHEDULER"][
                                      "ConverterOptions"] + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir
            self.scheduler = LocalScheduler(self.programCommand, self.programName, self.logger)
        elif "TORQUE_SCHEDULER" in sections:

            if "ProgramPath" not in config["TORQUE_SCHEDULER"]:
                self.logger.error("ProgramPath not found in TORQUE_SCHEDULER section")
                exit(1)
            if "ProgramName" in config["TORQUE_SCHEDULER"]:
                self.programName = config["TORQUE_SCHEDULER"]["ProgramName"]
            else:
                self.programName = config["TORQUE_SCHEDULER"]["ProgramPath"].split("/")[-1]

            nTasks = 0
            if "Ntasks" in config["TORQUE_SCHEDULER"]:
                nTasks = int(config["TORQUE_SCHEDULER"]["Ntasks"])
            else:
                self.logger.error("Ntasks not found in TORQUE_SCHEDULER section")
            qsubParameters = config["TORQUE_SCHEDULER"]["QsubParameters"]
            with open(self.tmpDir + "/qsubScript.sh", "w") as qsubScript:
                qsubScript.write("#!/bin/sh\n\n")
                if "QsubParameters" in config["TORQUE_SCHEDULER"]:
                    qsubScript.write("#PBS " + qsubParameters + "\n\n")
                qsubScript.write(shutil.which("mpiexec") + "-n " + str(nTasks) + " " + config["TORQUE_SCHEDULER"]["ProgramPath"] + " " +
                                   config["TORQUE_SCHEDULER"][
                                       "ConverterOptions"] + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir)
            self.programCommand = shutil.which("qsub") + " " + self.tmpDir + "/qsubScript.sh"
            self.scheduler = QsubScheduler(self.programCommand, self.programName, self.logger)


    def bordered(self, text):
        lines = text.split("\n")
        width = max(len(s) for s in lines)
        res = ['=' * width]
        for s in lines:
            res.append('||' + (s + ' ' * width)[:width] + '||')
        res.append('=' * width)
        return '\n'.join(res)

    def catFiles(self, filesToCat, destinationFile, batchCopier):

        batchCopier.concatFiles(filesToCat, destinationFile)
        batchCopier.removeFiles(filesToCat)

    def testBatchDone(self, batchCopier, signalDir, state):
        for signal in glob.glob(signalDir + "/batchno_*_" + state + "_time_written"):
            batchNum = int(Path(signal).stem.split("_")[1])
            signalFile = open(signalDir + "/batchno_" + str(batchNum) + "_" + state + "_done", "r")

            # line2 = ""
            # while line2 != "msgcomplete":
            #     time.sleep(1)
            #     signalFile.readline()
            #     line2 = signalFile.readline()
            #     signalFile.seek(0)

            duration = float(signalFile.readline().rstrip("\n"))
            signalFile.close()
            #with open(signal, "r") as signalFile:
              #  duration = float(signalFile.read())

            self.logger.info("Batch-" + str(batchNum) + "/Partition-" + str(
                batchCopier.getPartitionNum(batchNum)) + " done " + state + " in " + str(
                duration) + " seconds", color="green")
            self.batchesDone[state].append(str(batchNum) + "/" + str(batchCopier.getPartitionNum(batchNum)))
            self.numFilesDone[state] += len(batchCopier.getFiles(batchNum))
            # spaceleft += size
            batchCopier.removeBatch(batchNum)
            batchCopier.setTotalSize(max(0, batchCopier.gettotalSize()))
            Path(signalDir + "/batchno_" + str(batchNum) + "_" + state + "_done").unlink()
            Path(signal).unlink()
            if state == "indexing":
                filesToCat = []
                for octree in glob.glob(self.tmpOutputDir + "/octree*.bin"):
                    filesToCat.append(octree)
                filesToCat.sort(key=lambda x: int(Path(x).stem.split("_")[1]), reverse=True)
                self.catFiles(filesToCat, self.OutputDir + "/octree.bin", batchCopier)
                open(self.tmpOutputDir + "/indexing_copy_done_signals/batchno_" + str(batchNum) + "_concatenated",
                     "w").close()

    def copyBatch(self, filesToCopy, size, destination, batchCopier, signalDir, state, partition=""):
        if self.tmpInputDir != self.InputDir:
            batchCopier.copyBatch(filesToCopy, size, destination, partition)
        else:
            batchCopier.copyBatch([], 0, destination, partition)
        if batchCopier.gettotalSize() > self.maxTmpSpaceAvailable:
            self.logger.warning("Space utilization exceeded in " + state + " after copying batch " + str(
                batchCopier.getNumBatchesCopied() - 1) + ". Total space utilization:" + str(
                batchCopier.gettotalSize()) + " bytes, Max allowed:" + str(
                self.maxTmpSpaceAvailable) + " bytes")
        with open(signalDir + "/" + "batchno_" + str(batchCopier.getNumBatchesCopied() - 1) + "_copied",
                  "w") as signalToPotreeConverterMPI:  # signal to potree converter
            signalToPotreeConverterMPI.write(",".join(list(map(lambda x: str(destination + "/" + Path(x).name), filesToCopy))))  # write the copied files to the signal file
            if (self.lazFilestoProcess == []):
                signalToPotreeConverterMPI.write("\nlastbatch")
            else:
                signalToPotreeConverterMPI.write("\nnotlastbatch")
        open(signalDir + "/" + "batchno_" + str(batchCopier.getNumBatchesCopied() - 1) + "_written", "w").close()

    def counting(self, lazPartitions):
        self.logger.info("Batch copier for counting started", color="blue", bold=True)
        for partition in lazPartitions:
            self.lazFilestoProcess.extend(partition["files"])#glob.glob(self.InputDir + "/*.[lL][aA][zZ]")

        while self.lazFilestoProcess:  # loop until all files are copied
            if self.countingBatchCopier.gettotalSize() < self.maxTmpSpaceAvailable:
                size = 0
                filesToCopy = []
                currlazFilesforCount = self.lazFilestoProcess.copy()
                for laz in currlazFilesforCount:
                    p = Path(laz)
                    size += p.stat().st_size
                    if size <= self.countingBatchSize and self.countingBatchCopier.gettotalSize() <= self.maxTmpSpaceAvailable:
                        filesToCopy.append(laz)
                        self.lazFilestoProcess.remove(laz)
                    else:
                        break

                self.copyBatch(filesToCopy, size, self.tmpInputDir, self.countingBatchCopier, self.tmpOutputDir + "/counting_copy_done_signals", "counting")
            self.testBatchDone(self.countingBatchCopier, self.tmpOutputDir + "/counting_done_signals", "counting")
            if not self.countingBatchCopier.isbatchDictEmpty():
                self.logger.info("Counting pending for batches/partitions: " + ",".join(
                    map(lambda x: str(x) + "/" + str(self.countingBatchCopier.getPartitionNum(x)),
                        self.countingBatchCopier.getCopiedBatchesKeys())) + ". Done batches/partitions: " + ",".join(
                    self.batchesDone["counting"]))
                time.sleep(60)
        while not self.countingBatchCopier.isbatchDictEmpty():
            self.testBatchDone(self.countingBatchCopier, self.tmpOutputDir + "/counting_done_signals", "counting")

            if not self.countingBatchCopier.isbatchDictEmpty():
                self.logger.info("Counting pending for batches/partitions: " + ",".join(
                    map(lambda x: str(x) + "/" + str(self.countingBatchCopier.getPartitionNum(x)),
                        self.countingBatchCopier.getCopiedBatchesKeys())) + ". Done batches/partitions: " + ",".join(
                    self.batchesDone["counting"]))
                time.sleep(60)

        self.logger.info("Counting finished. Total " + str(
            self.numFilesDone["counting"]) + " files counted in " + str(
            len(self.batchesDone["counting"])) + " batches.", color="green", bold=True)
        self.lazFilestoProcess.clear()

    def indexing(self, batches):
        self.logger.info("Batch copier for indexing started", color="blue", bold=True)
        self.lazFilestoProcess = batches
        skipPartitions = []
        indexingDone = []
        numFilesdone = 0
        numBatches = 0
        totalBatches = len(self.lazFilestoProcess)
        while self.lazFilestoProcess:  # loop until all files are copied
            if self.indexingBatchCopier.gettotalSize() < self.maxTmpSpaceAvailable:
                currbatches = batches.copy()
                for lazBatch in currbatches:
                    if lazBatch["status"] == "skip":
                        skipPartitions.append(lazBatch)
                        self.lazFilestoProcess.remove(lazBatch)
                        self.logger.warning("Skipping partition " + str(lazBatch["id"]) + ". Too big for max tmp space allowed, size: " + str(lazBatch["size"]) + " bytes, Tmp space available: " + str(self.maxTmpSpaceAvailable) + " bytes, Tmp space requrired: + " + str(self.lazCompressionRatio * lazBatch["size"]) +" bytes")
                        continue
                    if (self.lazCompressionRatio * lazBatch["size"]) + (
                    self.indexingBatchCopier.gettotalSize()) <= self.maxTmpSpaceAvailable:
                        batchToCopy = lazBatch.copy()
                        self.lazFilestoProcess.remove(lazBatch)
                        self.copyBatch(batchToCopy["files"], batchToCopy["size"], self.tmpInputDir, self.indexingBatchCopier,
                                       self.tmpOutputDir + "/indexing_copy_done_signals", "indexing", batchToCopy["id"])
                        if self.indexingBatchCopier.gettotalSize() > self.maxTmpSpaceAvailable:
                            self.logger.warning("Space utilization exceeded in indexing after copying partition " + str(
                                batchToCopy["id"]) + ". Total space utilization:" + str(
                                    self.indexingBatchCopier.gettotalSize()) + " bytes, Max allowed:" + str(
                                    self.maxTmpSpaceAvailable) + " bytes")

            self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")
            if not self.indexingBatchCopier.isbatchDictEmpty():
                self.logger.info("Indexing pending for batches/partitions: " + ",".join(
                    map(lambda x: str(x) + "/" + str(self.indexingBatchCopier.getPartitionNum(x)),
                        self.indexingBatchCopier.getCopiedBatchesKeys())) + ". Skipped partitions :" + ",".join(
                    map(lambda x: str(x["id"]), skipPartitions)) + ". Done batches/partitions: " + ",".join(
                    self.batchesDone["indexing"]))
                time.sleep(60)

        while not self.indexingBatchCopier.isbatchDictEmpty():
            self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")
            if not self.indexingBatchCopier.isbatchDictEmpty():
                self.logger.info("Indexing pending for batches/partitions: " + ",".join(
                    map(lambda x: str(x) + "/" + str(self.indexingBatchCopier.getPartitionNum(x)),
                        self.indexingBatchCopier.getCopiedBatchesKeys())) + ". Skipped partitions:" + ",".join(
                    map(lambda x: str(x["id"]), skipPartitions)) + ". Done batches/partitions: " + ",".join(
                    indexingDone))
                time.sleep(60)
        self.logger.info("Indexing finished. Total " + str(
            self.numFilesDone["indexing"]) + " files indexed in " + str(
            len(self.batchesDone["indexing"])) + " partitions. Total " + str(totalBatches) + " partitions, " + str(
            len(skipPartitions)) +  " skipped" , color="green", bold=True)

    def createPartitions(self):
        self.logger.info("Creating partitions...", color="blue", bold=True)

        lazfileStats = {}
        with open(self.partitionsCSV, "r", newline='') as partitionFile:
            partition = csv.reader(partitionFile)
            colName = next(partition)
            bladnrIdx = colName.index("bladnr")
            partitionIdIdx = colName.index("partition_id")
            for row in partition:
                if row[partitionIdIdx] not in lazfileStats:
                    lazfileStats[row[partitionIdIdx]] = {}
                    lazfileStats[row[partitionIdIdx]]["bladnr"] = [row[bladnrIdx]]
                else:
                    lazfileStats[row[partitionIdIdx]]["bladnr"].append(row[bladnrIdx])

        lazPartitions = []

        for id in lazfileStats:
            partitionSize = sum((Path(self.InputDir + "/C_" + bladnr.upper() + ".LAZ").stat().st_size) for bladnr in lazfileStats[id]["bladnr"])
            filesinPartition = {}
            filesinPartition["size"] = partitionSize
            filesinPartition["files"] = []
            filesinPartition["id"] = id
            for bladnr in lazfileStats[id]["bladnr"]:
                filesinPartition["files"] .append(self.InputDir + "/C_" + bladnr.upper() + ".LAZ")
            if self.lazCompressionRatio * partitionSize <= self.maxTmpSpaceAvailable:
                filesinPartition["status"] = "do"
            else:
                filesinPartition["status"] = "skip"
                self.logger.warning(f"Partition {id} too big for max tmp space allowed. It will be skipped. Size: {str(partitionSize)} bytes, Tmp space available: {str(self.maxTmpSpaceAvailable)} bytes, Tmp space requrired: {str(self.lazCompressionRatio * partitionSize)} bytes")

            lazPartitions.append(filesinPartition)
        self.logger.info("Done creating partitions. Total partitions: " + str(len(lazPartitions)), color="green", bold=True)

        return lazPartitions

    def createDirectories(self):

        self.logger.info("Creating directories...", color="blue", bold=True)
        if not Path(self.InputDir).exists():
            self.logger.error("Input directory  does not exist")
            exit(1)
        elif not Path(self.lazHeadersToCopy).exists():
            self.logger.error("Headers directory for input data does not exist")
            exit(1)
        else:
            pathsCount = 0
            for path in glob.glob(self.lazHeadersToCopy + "/*.[jJ][sS][oO][nN]"):
                if not glob.glob(self.InputDir + "/" + Path(path).stem + ".[lL][aA][zZ]"):
                    self.logger.error("LAZ file for " + Path(path).name + " in the input directory" + self.InputDir + " does not exist")
                    exit(1)
                pathsCount += 1
            if pathsCount == 0:
                self.logger.error("No LAZ files to process in the input directory")
                exit(1)

        if Path(self.tmpDir).exists():
            print("Directory for temporarily storing partial input and output already exists. Do you want to overwrite it? (y/n)")
            answer = input()
            if answer == "y" or answer == "Y":
                self.logger.info("Removing directory for temporarily storing partial input and output...")
                shutil.rmtree(self.tmpDir)
            else:
                self.logger.info("Directory for temporarily storing partial input and output already exists. Exiting on user request...")
                exit(1)
        self.logger.info("Creating directory for temporarily storing partial input and output...")
        Path(self.tmpDir).mkdir()
        if self.tmpInputDir != self.InputDir:
            Path(self.tmpInputDir).mkdir()
        Path(self.tmpOutputDir).mkdir()

        if Path(self.OutputDir).exists():
            print("Output directory already exists. Do you want to overwrite it? (y/n)")
            answer = input()
            if answer == "y" or answer == "Y":
                self.logger.info("Removing output directory...")
                shutil.rmtree(self.OutputDir)
            else:
                self.logger.info(
                    "Output directory already exists. Exiting on user request... " )
                exit(1)
        self.logger.info("Creating output directory...")
        Path(self.OutputDir).mkdir()
        self.logger.info("Creating directory for headers...")
        Path(self.lazHeadersDir).mkdir()
        self.logger.info("Copying headers...", color="blue", bold=True)
        self.miscCopier.copyFiles(glob.glob(self.lazHeadersToCopy + "/*.json"), self.lazHeadersDir)
        self.logger.info("Done copying headers", color="green", bold=True)
        Path(self.tmpOutputDir + "/counting_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/indexing_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/counting_done_signals").mkdir()
        Path(self.tmpOutputDir + "/indexing_done_signals").mkdir()
        self.logger.info("Done creating directories", color="green", bold=True)

    def removeDirectories(self, directories):
        for directory in directories:
            shutil.rmtree(directory)


def PotreeConverterMPIBatchedCopier(potreeConverterBatched):

    lazpartitions = potreeConverterBatched.createPartitions()
    potreeConverterBatched.counting(lazpartitions)
    potreeConverterBatched.indexing(lazpartitions)

    exit(0)


if __name__ == "__main__":


    if len(sys.argv) != 2:
        print("Usage: python3 PotreeConverterBatched.py <config_file>")
        exit(1)
    configFilePath = sys.argv[1]
    potreeConverterBatched = PotreeConverterBatched(configFilePath)
    potreeConverterBatched.logger.info("Starting PotreeConverterMPI Batched", color="blue", bold=True)
    #potreeConverterBatched.createDirectories()
    potreeConverterBatched.scheduler.launchJob()

    # PotreeConverterMPIMonitorProcess = Process(target=potreeConverterBatched.scheduler.monitorJob)
    # PotreeConverterMPIMonitorProcess.start()
    PotreeConverterMPICopier = Process(target=PotreeConverterMPIBatchedCopier, args=(potreeConverterBatched,))
    PotreeConverterMPICopier.start()

    while potreeConverterBatched.scheduler.isJobAlive() and PotreeConverterMPICopier.is_alive():
        time.sleep(10)

    if potreeConverterBatched.scheduler.isJobAlive() and PotreeConverterMPICopier.exitcode != 0 :
        potreeConverterBatched.logger.error("Batch copier failed")
        potreeConverterBatched.scheduler.killJob()
        exit(1)
    elif potreeConverterBatched.scheduler.getJobExitCode() != 0 and PotreeConverterMPICopier.is_alive():
        parent = psutil.Process(PotreeConverterMPICopier.pid)
        for child in parent.children(recursive=True):
            child.kill()
        PotreeConverterMPICopier.terminate()
        potreeConverterBatched.logger.info("Batch copier terminated")
        exit(1)
    elif potreeConverterBatched.scheduler.getJobExitCode() != 0 and PotreeConverterMPICopier.exitcode != 0:
        potreeConverterBatched.logger.error("Batch copier and PotreeConverterMPI  failed")
        exit(1)
    elif potreeConverterBatched.scheduler.isJobAlive()  and PotreeConverterMPICopier.exitcode == 0:
        potreeConverterBatched.logger.info("Batch copier finished successfully. Waiting for PotreeConverterMPI to finish...", color="green")
        while potreeConverterBatched.scheduler.isJobAlive():
            time.sleep(10)
        if potreeConverterBatched.scheduler.getJobExitCode() != 0:
            potreeConverterBatched.logger.error("PotreeConverterMPI failed. However, batch copier finished successfully")
            exit(1)
        else:
            potreeConverterBatched.logger.info("Batch copier and PotreeConverterMPI finished successfully", color="green", bold=True)
    elif potreeConverterBatched.scheduler.getJobExitCode() == 0 and PotreeConverterMPICopier.is_alive():
        potreeConverterBatched.logger.info("PotreeConverterMPI finished successfully. Waiting for batch copier to finish...", color="green")
        while PotreeConverterMPICopier.is_alive():
            time.sleep(10)
        if PotreeConverterMPICopier.exitcode != 0:
            potreeConverterBatched.logger.error("Batch copier failed. However, PotreeConverterMPI finished successfully")
            exit(1)
        else:
            potreeConverterBatched.logger.info("Batch copier  and PotreeConverterMPI finished successfully", color="green", bold=True)
    else:
        potreeConverterBatched.logger.info("Batch copier and PotreeConverterMPI finished successfully", color="green", bold=True)

        # potreeConverterBatched.logger.info("Removing directories for batch copier")
        # potreeConverterBatched.removeDirectories([potreeConverterBatched.tmpInputDir, potreeConverterBatched.lazHeadersDir])
