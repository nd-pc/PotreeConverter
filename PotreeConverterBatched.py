

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
        self.overallSizeRatio = 0
        self.inputToOutputSizeRatio = 0
        self.inputToTmpInputSizeRatio = 0
        self.tmpDir = ""
        self.tmpInputDir = ""
        self.tmpOutputDir = ""
        self.InputDir = ""
        self.OutputDir = ""
        self.lazHeadersDir = ""
        self.lazHeadersToCopy = ""
        self.partitionsCSV = ""
        self.copierType = ""
        self.countingBatchCopier = None
        self.indexingBatchCopier = None
        self.miscCopier = None
        self.logFile = ""
        self.batchesDone = {"counting": [], "indexing": []}
        self.numFilesDone = {"counting": 0, "indexing": 0}
        self.lazFilestoProcess = []
        self.programName = ""
        self.programPath = ""
        self.programOptions = ""
        self.concatOutput = True
        self.programPath = ""

        config = configparser.ConfigParser()
        config.read(configFile)
        sections = config.sections()
        if "INPUT_OUTPUT" not in sections:
            LoggingWrapper.error("INPUT_OUTPUT section not found in config file")
            exit(1)
        else:
            if "InputDir" not in config["INPUT_OUTPUT"]:
                LoggingWrapper.error("InputDir not found in INPUT_OUTPUT section")
                exit(1)
            else:
                self.InputDir = config["INPUT_OUTPUT"]["InputDir"]
            if "InputDirType" not in config["INPUT_OUTPUT"]:
                LoggingWrapper.error("InputDirType not found in INPUT_OUTPUT section")
                exit(1)
            if "OutputDir" not in config["INPUT_OUTPUT"]:
                LoggingWrapper.error("OutputDir not found in INPUT_OUTPUT section")
                exit(1)
            else:
                self.OutputDir = config["INPUT_OUTPUT"]["OutputDir"]
                if "OutputDirType" not in config["INPUT_OUTPUT"]:
                    LoggingWrapper.error("OutputDirType not found in INPUT_OUTPUT section")
                    exit(1)

            if "LazHeadersDir" not in config["INPUT_OUTPUT"]:
                LoggingWrapper.error("LazHeadersDir not found in INPUT_OUTPUT section")
                exit(1)
            else:
                self.lazHeadersToCopy = config["INPUT_OUTPUT"]["LazHeadersDir"]
        if "PARTITIONS" not in sections:
            LoggingWrapper.error("PARTITIONS section not found in config file")
            exit(1)
        else:
            if "CSV" not in config["PARTITIONS"]:
                LoggingWrapper.error("CSV not found in PARTITIONS section")
                exit(1)
            else:
                self.partitionsCSV = config["PARTITIONS"]["CSV"]

            self.countingBatchSize = eval(config["PARTITIONS"]["CountingBatchSize"])

        if "TMP_STORAGE" not in sections:
            LoggingWrapper.error("TMP_STORAGE section not found in config file")
            exit(1)
        else:
                if "TmpDir" not in config["TMP_STORAGE"]:
                    LoggingWrapper.error("TmpDir not found in TMP_STORAGE section")
                    exit(1)
                else:
                    self.tmpDir = config["TMP_STORAGE"]["TmpDir"]
                    if config["INPUT_OUTPUT"]["InputDirType"] == "local":
                        self.tmpInputDir = self.InputDir
                        self.inputToTmpInputSizeRatio = 0
                    else:
                        self.tmpInputDir = self.tmpDir + "/PotreeConverter_Input"
                        self.inputToTmpInputSizeRatio = 1
                    if config["INPUT_OUTPUT"]["OutputDirType"] == "local":
                        self.tmpOutputDir = self.OutputDir
                        self.concatOutput = False
                        self.inputToOutputSizeRatio = 0
                    else:
                        self.tmpOutputDir = self.tmpDir + "/PotreeConverter_Output"
                        self.concatOutput = True
                        self.inputToOutputSizeRatio = 2

                if "MaxTmpSpaceAvailable" not in config["TMP_STORAGE"]:
                    LoggingWrapper.error(
                        "MaxTmpSpaceAvailable not found in TMP_STORAGE section")
                    exit(1)
                else:
                    self.maxTmpSpaceAvailable = eval(config["TMP_STORAGE"]["MaxTmpSpaceAvailable"])

                self.overallSizeRatio = eval(config["TMP_STORAGE"]["LazCompressionRatio"]) + self.inputToOutputSizeRatio + self.inputToTmpInputSizeRatio

        self.copierType = config["COPIER"]["CopierType"]
        if self.copierType == "local":
             self.countingBatchCopier = LocalCopier()
             self.indexingBatchCopier = LocalCopier()
             self.miscCopier = LocalCopier()
        else:
            LoggingWrapper.error("Copier type not recognized. Must be only local")
            exit(1)

        self.createDirectories()

        if "PROGRAM" not in sections:
            LoggingWrapper.error("PROGRAM section not found in config file")
            exit(1)
        else:
            if "ProgramPath" not in config["PROGRAM"]:
                LoggingWrapper.error("ProgramPath not found in SCHEDULER section")
                exit(1)
            else:
                self.programPath = config["PROGRAM"]["ProgramPath"]
                self.programName = config["PROGRAM"]["ProgramPath"].split("/")[-1]
            if "Options" in config["PROGRAM"]:
                self.programOptions = config["PROGRAM"]["Options"]


        if "SCHEDULER" in sections:
            if "Parameters" not in config["SCHEDULER"]:
                LoggingWrapper.error("Parameters not found in SCHEDULER section")
                exit(1)
            if "Type" in config["SCHEDULER"]:
                if config["Scheduler"]["Type"] == "sbatch":
                    parameters = config["SCHEDULER"]["Parameters"]
                    with open(self.tmpDir + "/sbatchScript.sh", "w") as sbatchScript:
                        sbatchScript.write("#!/bin/sh\n\n")
                        sbatchScript.write("#SBATCH " + parameters + "\n\n")
                        sbatchScript.write(self.programPath + " --encoding BROTLI " +
                                           self.programOptions + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir +  " --concatOutput" if self.concatOutput else "")
                    programCommand = shutil.which("sbatch") + " " + self.tmpDir + "/sbatchScript.sh"
                    self.scheduler = SbatchScheduler(programCommand, self.programName, LoggingWrapper)
                elif config["Scheduler"]["Type"] == "pbs":
                    parameters = config["SCHEDULER"]["Parameters"]
                    with open(self.tmpDir + "/qsubScript.sh", "w") as qsubScript:
                        qsubScript.write("#!/bin/sh\n\n")
                        qsubScript.write("#PBS " + parameters + "\n\n")
                        qsubScript.write(self.programPath + " --encoding BROTLI " +
                                         self.programOptions + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir + " --concatOutput" if self.concatOutput else "")
                    programCommand = shutil.which("qsub") + " " + self.tmpDir + "/qsubScript.sh"
                    self.scheduler = QsubScheduler(programCommand, self.programName, LoggingWrapper)
                elif config["Scheduler"]["Type"] == "local":
                    programCommand = self.programPath + " --encoding BROTLI " + self.programOptions + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir + " --concatOutput" if self.concatOutput else ""
                    self.scheduler = LocalScheduler(programCommand, self.programName, LoggingWrapper)
                else:
                    LoggingWrapper.error("Scheduler type not recognized. Must be one of sbatch, pbs or local")
                    exit(1)

        else:
            LoggingWrapper.error("SCHEDULER section not found in config file")
            exit(1)




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

            LoggingWrapper.info("Batch-" + str(batchNum) + "/Partition-" + str(
                batchCopier.getPartitionNum(batchNum)) + " done " + state + " in " + str(
                duration) + " seconds", color="green")
            self.batchesDone[state].append(str(batchNum) + "/" + str(batchCopier.getPartitionNum(batchNum)))
            self.numFilesDone[state] += len(batchCopier.getFiles(batchNum))
            # spaceleft += size
            batchCopier.removeBatch(batchNum)
            batchCopier.setTotalSize(max(0, batchCopier.gettotalSize()))
            Path(signalDir + "/batchno_" + str(batchNum) + "_" + state + "_done").unlink()
            Path(signal).unlink()
            if state == "indexing" and self.concatOutput:
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
            LoggingWrapper.warning("Space utilization exceeded in " + state + " after copying batch " + str(
                batchCopier.getNumBatchesCopied() - 1) + ". Total space utilization:" + str(
                batchCopier.gettotalSize()) + " bytes, Max allowed:" + str(
                self.maxTmpSpaceAvailable) + " bytes")
        with open(signalDir + "/" + "batchno_" + str(batchCopier.getNumBatchesCopied() - 1) + "_copied",
                  "w") as signalToPotreeConverter:  # signal to potree converter
            signalToPotreeConverter.write(",".join(list(map(lambda x: str(destination + "/" + Path(x).name), filesToCopy))))  # write the copied files to the signal file
            if (self.lazFilestoProcess == []):
                signalToPotreeConverter.write("\nlastbatch")
            else:
                signalToPotreeConverter.write("\nnotlastbatch")
        open(signalDir + "/" + "batchno_" + str(batchCopier.getNumBatchesCopied() - 1) + "_written", "w").close()

    def counting(self, lazPartitions):
        LoggingWrapper.info("Batch copier for counting started", color="blue", bold=True)
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
                LoggingWrapper.info("Counting pending for batches/partitions: " + ",".join(
                    map(lambda x: str(x) + "/" + str(self.countingBatchCopier.getPartitionNum(x)),
                        self.countingBatchCopier.getCopiedBatchNums())) + ". Done batches/partitions: " + ",".join(
                    self.batchesDone["counting"]))
                time.sleep(60)
        while not self.countingBatchCopier.isbatchDictEmpty():
            self.testBatchDone(self.countingBatchCopier, self.tmpOutputDir + "/counting_done_signals", "counting")

            if not self.countingBatchCopier.isbatchDictEmpty():
                LoggingWrapper.info("Counting pending for batches/partitions: " + ",".join(
                    map(lambda x: str(x) + "/" + str(self.countingBatchCopier.getPartitionNum(x)),
                        self.countingBatchCopier.getCopiedBatchNums())) + ". Done batches/partitions: " + ",".join(
                    self.batchesDone["counting"]))
                time.sleep(60)

        LoggingWrapper.info("Counting finished. Total " + str(
            self.numFilesDone["counting"]) + " files counted in " + str(
            len(self.batchesDone["counting"])) + " batches.", color="green", bold=True)
        self.lazFilestoProcess.clear()

    def indexing(self, batches):
        LoggingWrapper.info("Batch copier for indexing started", color="blue", bold=True)
        self.lazFilestoProcess = batches
        skipPartitions = []
        indexingDone = []
        numFilesdone = 0
        numBatches = 0
        totalBatches = len(self.lazFilestoProcess)
        while self.lazFilestoProcess:  # loop until all files are copied
            if not self.indexingBatchCopier.getCopiedBatchNums():
                nextBatchSize = 0
            else:
                nextBatchSize = self.indexingBatchCopier.getBatchSize(min(self.indexingBatchCopier.getCopiedBatchNums()))
            if ((nextBatchSize*self.overallSizeRatio) + (self.indexingBatchCopier.gettotalSize() - self.indexingBatchCopier.getMaxBatchSize())) <= self.maxTmpSpaceAvailable:
            #if self.indexingBatchCopier.gettotalSize() < self.maxTmpSpaceAvailable:
                currbatches = batches.copy()
                for lazBatch in currbatches:
                    if lazBatch["status"] == "skip":
                        skipPartitions.append(lazBatch)
                        self.lazFilestoProcess.remove(lazBatch)
                        LoggingWrapper.warning("Skipping partition " + str(lazBatch["id"]) + ". Too big for max tmp space allowed, size: " + str(lazBatch["size"]) + " bytes, Tmp space available: " + str(self.maxTmpSpaceAvailable) + " bytes, Tmp space requrired: + " + str(self.overallSizeRatio * lazBatch["size"]) +" bytes")
                        continue
                    if (self.overallSizeRatio * lazBatch["size"]) + (
                    self.indexingBatchCopier.gettotalSize()) <= self.maxTmpSpaceAvailable:
                        batchToCopy = lazBatch.copy()
                        self.lazFilestoProcess.remove(lazBatch)
                        self.copyBatch(batchToCopy["files"], batchToCopy["size"], self.tmpInputDir, self.indexingBatchCopier,
                                       self.tmpOutputDir + "/indexing_copy_done_signals", "indexing", batchToCopy["id"])
                        if self.indexingBatchCopier.gettotalSize() > self.maxTmpSpaceAvailable:
                            LoggingWrapper.warning("Space utilization exceeded in indexing after copying partition " + str(
                                batchToCopy["id"]) + ". Total space utilization:" + str(
                                    self.indexingBatchCopier.gettotalSize()) + " bytes, Max allowed:" + str(
                                    self.maxTmpSpaceAvailable) + " bytes")

            self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")
            if not self.indexingBatchCopier.isbatchDictEmpty():
                LoggingWrapper.info("Indexing pending for batches/partitions: " + ",".join(
                    map(lambda x: str(x) + "/" + str(self.indexingBatchCopier.getPartitionNum(x)),
                        self.indexingBatchCopier.getCopiedBatchNums())) + ". Skipped partitions :" + ",".join(
                    map(lambda x: str(x["id"]), skipPartitions)) + ". Done batches/partitions: " + ",".join(
                    self.batchesDone["indexing"]))
                time.sleep(60)

        while not self.indexingBatchCopier.isbatchDictEmpty():
            self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")
            if not self.indexingBatchCopier.isbatchDictEmpty():
                LoggingWrapper.info("Indexing pending for batches/partitions: " + ",".join(
                    map(lambda x: str(x) + "/" + str(self.indexingBatchCopier.getPartitionNum(x)),
                        self.indexingBatchCopier.getCopiedBatchNums())) + ". Skipped partitions:" + ",".join(
                    map(lambda x: str(x["id"]), skipPartitions)) + ". Done batches/partitions: " + ",".join(
                    indexingDone))
                time.sleep(60)
        LoggingWrapper.info("Indexing finished. Total " + str(
            self.numFilesDone["indexing"]) + " files indexed in " + str(
            len(self.batchesDone["indexing"])) + " partitions. Total " + str(totalBatches) + " partitions, " + str(
            len(skipPartitions)) +  " skipped" , color="green", bold=True)

    def createPartitions(self):
        LoggingWrapper.info("Creating partitions...", color="blue", bold=True)

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
            if self.overallSizeRatio * partitionSize <= self.maxTmpSpaceAvailable:
                filesinPartition["status"] = "do"
            else:
                filesinPartition["status"] = "skip"
                LoggingWrapper.warning(f"Partition {id} too big for max tmp space allowed. It will be skipped. Size: {str(partitionSize)} bytes, Tmp space available: {str(self.maxTmpSpaceAvailable)} bytes, Tmp space requrired: {str(self.overallSizeRatio * partitionSize)} bytes")

            lazPartitions.append(filesinPartition)
        LoggingWrapper.info("Done creating partitions. Total partitions: " + str(len(lazPartitions)), color="green", bold=True)

        return lazPartitions

    def createDirectories(self):

        LoggingWrapper.info("Creating directories...", color="blue", bold=True)
        if not Path(self.InputDir).exists():
            LoggingWrapper.error("Input directory " + self.InputDir + " does not exist")
            exit(1)
        elif not Path(self.lazHeadersToCopy).exists():
            LoggingWrapper.error("Headers directory " + self.lazHeadersToCopy + " does not exist")
            exit(1)
        else:
            pathsCount = 0
            for path in glob.glob(self.lazHeadersToCopy + "/*.[jJ][sS][oO][nN]"):
                if not glob.glob(self.InputDir + "/" + Path(path).stem + ".[lL][aA][zZ]"):
                    LoggingWrapper.error("LAZ file for " + Path(path).name + " in the input directory " + self.InputDir + " does not exist")
                    exit(1)
                pathsCount += 1
            if pathsCount == 0:
                LoggingWrapper.error("No LAZ files to process in the input directory")
                exit(1)

        if Path(self.tmpDir).exists():
            print("Directory "  + self.tmpDir  + " for temporarily storing partial input and output already exists. Do you want to overwrite it? (y/n)")
            answer = input()
            if answer == "y" or answer == "Y":
                LoggingWrapper.info("Removing directory " + self.tmpDir + " for temporarily storing partial input and output...")
                shutil.rmtree(self.tmpDir)
            else:
                LoggingWrapper.info("Directory " + self.tmpDir + " for temporarily storing partial input and output already exists. Exiting on user request... ")
                exit(1)
        LoggingWrapper.info("Creating directory " + self.tmpDir + " for temporarily storing partial input and output...")
        Path(self.tmpDir).mkdir()
        if self.tmpInputDir != self.InputDir:
            Path(self.tmpInputDir).mkdir()
        if self.tmpOutputDir != self.OutputDir:
            Path(self.tmpOutputDir).mkdir()

        if Path(self.OutputDir).exists() and self.tmpOutputDir != self.OutputDir:
            print("Output directory " + self.OutputDir + " already exists. Do you want to overwrite it? (y/n)")
            answer = input()
            if answer == "y" or answer == "Y":
                LoggingWrapper.info("Removing output directory " + self.OutputDir + "...")
                shutil.rmtree(self.OutputDir)
            else:
                LoggingWrapper.info(
                    "Output directory " + self.OutputDir + " already exists. Exiting on user request... ")
                exit(1)
        LoggingWrapper.info("Creating output directory " + self.OutputDir + "...")
        Path(self.OutputDir).mkdir()
        LoggingWrapper.info("Creating directory " + self.lazHeadersDir + " for temporarily storing headers...")
        Path(self.lazHeadersDir).mkdir()
        LoggingWrapper.info("Copying headers...", color="blue", bold=True)
        self.miscCopier.copyFiles(glob.glob(self.lazHeadersToCopy + "/*.json"), self.lazHeadersDir)
        LoggingWrapper.info("Done copying headers", color="green", bold=True)
        Path(self.tmpOutputDir + "/counting_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/indexing_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/counting_done_signals").mkdir()
        Path(self.tmpOutputDir + "/indexing_done_signals").mkdir()
        LoggingWrapper.info("Done creating directories", color="green", bold=True)

    def removeDirectories(self, directories):
        for directory in directories:
            shutil.rmtree(directory)


def PotreeConverterBatchedCopier(potreeConverterBatched):

    lazpartitions = potreeConverterBatched.createPartitions()
    potreeConverterBatched.counting(lazpartitions)
    potreeConverterBatched.indexing(lazpartitions)

    exit(0)


if __name__ == "__main__":


    if len(sys.argv) != 2:
        LoggingWrapper.error("Usage: python3 PotreeConverterBatched.py <config_file>")
        exit(1)
    configFilePath = sys.argv[1]
    potreeConverterBatched = PotreeConverterBatched(configFilePath)
    LoggingWrapper.info("Starting PotreeConverter Batched", color="blue", bold=True)
    #potreeConverterBatched.createDirectories()
    potreeConverterBatched.scheduler.launchJob()

    # PotreeConverterMonitorProcess = Process(target=potreeConverterBatched.scheduler.monitorJob)
    # PotreeConverterMonitorProcess.start()
    PotreeConverterCopier = Process(target=PotreeConverterBatchedCopier, args=(potreeConverterBatched,))
    PotreeConverterCopier.start()

    while potreeConverterBatched.scheduler.isJobAlive() and PotreeConverterCopier.is_alive():
        time.sleep(10)

    if potreeConverterBatched.scheduler.isJobAlive() and PotreeConverterCopier.exitcode != 0 :
        LoggingWrapper.error("Batch copier failed")
        potreeConverterBatched.scheduler.killJob()
        exit(1)
    elif potreeConverterBatched.scheduler.getJobExitCode() != 0 and PotreeConverterCopier.is_alive():
        parent = psutil.Process(PotreeConverterCopier.pid)
        for child in parent.children(recursive=True):
            child.kill()
        PotreeConverterCopier.terminate()
        LoggingWrapper.info("Batch copier terminated")
        exit(1)
    elif potreeConverterBatched.scheduler.getJobExitCode() != 0 and PotreeConverterCopier.exitcode != 0:
        LoggingWrapper.error("Batch copier and PotreeConverter  failed")
        exit(1)
    elif potreeConverterBatched.scheduler.isJobAlive()  and PotreeConverterCopier.exitcode == 0:
        LoggingWrapper.info("Batch copier finished successfully. Waiting for PotreeConverter to finish...", color="green")
        while potreeConverterBatched.scheduler.isJobAlive():
            time.sleep(10)
        if potreeConverterBatched.scheduler.getJobExitCode() != 0:
            LoggingWrapper.error("PotreeConverter failed. However, batch copier finished successfully")
            exit(1)
        else:
            LoggingWrapper.info("Batch copier and PotreeConverter finished successfully", color="green", bold=True)
    elif potreeConverterBatched.scheduler.getJobExitCode() == 0 and PotreeConverterCopier.is_alive():
        LoggingWrapper.info("PotreeConverter finished successfully. Waiting for batch copier to finish...", color="green")
        while PotreeConverterCopier.is_alive():
            time.sleep(10)
        if PotreeConverterCopier.exitcode != 0:
            LoggingWrapper.error("Batch copier failed. However, PotreeConverter finished successfully")
            exit(1)
        else:
            LoggingWrapper.info("Batch copier  and PotreeConverter finished successfully", color="green", bold=True)
    else:
        LoggingWrapper.info("Batch copier and PotreeConverter finished successfully", color="green", bold=True)
        LoggingWrapper.info("Output files written to directory: " + potreeConverterBatched.OutputDir, color="green", bold=True)

        # LoggingWrapper.info("Removing directories for batch copier")
        # potreeConverterBatched.removeDirectories([potreeConverterBatched.tmpInputDir, potreeConverterBatched.lazHeadersDir])
