import ast
import configparser
import glob
import sys
import heapq
from datetime import datetime
from pathlib import Path
import time
import csv
import psutil
import shutil
from scheduler import SbatchScheduler
from scheduler import LocalScheduler
from scheduler import QsubScheduler
from copier import ParallelCopier

from loggingwrapper import LoggingWrapper


from multiprocessing import Process




class PotreeConverterBatched:
    ''' The class is a driver for the PotreeConverterMPI program. It starts the PotreeConverterMPI program and load/unload the partitions '''
    def __init__(self, configFile):
        ''' The constructor reads the config file and initializes the class variables. It also starts the PotreeConverterMPI program '''
        #class variables
        self.maxTmpSpaceAvailable = 0
        self.countingBatchSize = 0
        self.distributionBatchSize = 0
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
        self.distributionBatchCopier = None
        self.miscCopier = None
        self.logFile = ""
        self.batchesDone = {"counting": [], "indexing": [], "distribution": []}
        self.numFilesDone = {"counting": 0, "indexing": 0, "distribution": 0}
        self.lazFilestoProcess = []
        self.lazDistributionBatch = []
        self.programCommand = ""
        self.programName = ""
        self.logger = None
        self.partitionsToProcess = None
        self.lazPartitions = []

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
            if "PartitionsToProcess" in config["INPUT_OUTPUT"]:
                self.partitionsToProcess = ast.literal_eval(config["INPUT_OUTPUT"]["PartitionsToProcess"])


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
                self.distributionBatchSize = eval(config["TMP_STORAGE"]["DistributionBatchSize"])
                self.lazCompressionRatio = eval(config["TMP_STORAGE"]["LazCompressionRatio"])
        if "COPIER" not in sections:
            self.copierType = config["DEFAULT"]["CopierType"]
        else:
            self.copierType = config["COPIER"]["CopierType"]

        if self.copierType == "local":
             self.countingBatchCopier = ParallelCopier(self.logger)
             self.distributionBatchCopier = ParallelCopier(self.logger)
             self.indexingBatchCopier =  ParallelCopier(self.logger)
             self.miscCopier =  ParallelCopier(self.logger)
        else:
            self.logger.error("Copier type not recognized")
            exit(1)

        self.createPartitions()
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
        ''' This function is used to print a bordered text '''
        lines = text.split("\n")
        width = max(len(s) for s in lines)
        res = ['=' * width]
        for s in lines:
            res.append('||' + (s + ' ' * width)[:width] + '||')
        res.append('=' * width)
        return '\n'.join(res)

    def catFiles(self, filesToCat, destinationFile, batchCopier):
        ''' This function is used to concatenate the output files of the PotreeConverterMPI program after processing a partition'''
        batchCopier.concatFiles(filesToCat, destinationFile)
        batchCopier.removeFiles(filesToCat)


    def testBatchDone(self, batchCopier, signalDir, state):
        ''' This function is used to test if a batch/partition is done processing. If a batch is done processing, it will be removed from the batch dictionary and the corresponding signal files will be removed'''
        if state == "indexing" or state == "distribution":
            partitionorbatch = "Partition"
        else:
            partitionorbatch = "Batch"
        doneBatchNums = []
        if glob.glob(signalDir + "/batchno_*_" + state + "_done") != []:
            doneBatchNums = sorted(map(lambda x: int(Path(x).stem.split("_")[1]), glob.glob(signalDir + "/batchno_*_" + state + "_time_written")))
        for batchNum in doneBatchNums:
            signalFile = open(signalDir + "/batchno_" + str(batchNum) + "_" + state + "_done", "r")
            durationStr = signalFile.readline().rstrip("\n")
            duration = float(durationStr)
            signalFile.close()
            if (durationStr != "-1.0"):
                self.logger.info(partitionorbatch + "-" + str(
                batchCopier.getPartitionNum(batchNum)) + " done " + state + " in " + str(
                duration) + " seconds", color="green")
            if state == "distribution":
                self.batchesDone[state].append(str(batchNum) + "(in partition-" + str(batchCopier.getPartitionNum(batchNum)) + ")")
            else:
                self.batchesDone[state].append(str(batchCopier.getPartitionNum(batchNum)))
            self.numFilesDone[state] += len(batchCopier.getFiles(batchNum))
            batchCopier.removeBatch(batchNum)
            batchCopier.setTotalSize(max(0, batchCopier.gettotalSize()))
            Path(signalDir + "/batchno_" + str(batchNum) + "_" + state + "_done").unlink()
            Path(signalDir + "/batchno_" + str(batchNum) + "_" + state + "_time_written").unlink()
            if state == "indexing":
                filesToCat = []
                for octree in glob.glob(self.tmpOutputDir + "/octree_" + str(batchNum) + "_*.bin"):
                    filesToCat.append(octree)
                filesToCat.sort(key=lambda x: int(Path(x).stem.split("_")[-1]), reverse=True)
                self.catFiles(filesToCat, self.OutputDir + "/octree.bin", batchCopier)
                open(self.tmpOutputDir + "/indexing_copy_done_signals/batchno_" + str(batchNum) + "_concatenated",
                     "w").close()
        if not batchCopier.isbatchDictEmpty():
            pending = ",".join(map(lambda x: str(batchCopier.getPartitionNum(x)), batchCopier.getCopiedBatchNums()))
            done = ",".join(self.batchesDone[state])
            if state == "distribution":
                pending = ",".join(map(lambda x: str(x) + "(in partition-" + str(batchCopier.getPartitionNum(x)) + ")", batchCopier.getCopiedBatchNums()))
            self.logger.info(state.capitalize() +  " pending for " + partitionorbatch.lower() + "s: " + pending + ". Done " + partitionorbatch.lower() + "s: " + done)
    def copyBatch(self, filesToCopy, size, destination, batchCopier, signalDir, state, partition=""):
        ''' This function is used to copy a batch/partition to the temporary input directory.
        :param filesToCopy: The list of files to copy
        :param size: The total size of the files to copy
        :param destination: The destination directory
        :param batchCopier: The copier object
        :param signalDir: The directory where to write signal files for the PotreeConverterMPI program
        :param state: The state of the batch/partition
        :param partition: The partition number
        '''

        if self.tmpInputDir != self.InputDir:
            batchCopier.copyBatch(filesToCopy, size, destination, partition)
        else:
            batchCopier.copyBatch([], 0, destination, partition)
        if state != "indexing":
            with open(signalDir + "/" + "batchno_" + str(batchCopier.getNumBatchesCopied() - 1) + "_copied",
                      "w") as signalToPotreeConverterMPI:  # signal to potree converter
                signalToPotreeConverterMPI.write(",".join(list(map(lambda x: str(destination + "/" + Path(x).name), filesToCopy))))  # write the copied files to the signal file
                if (state == "distribution"):
                    if self.lazDistributionBatch == []:
                        signalToPotreeConverterMPI.write("\nlastminibatchinpartition")
                    else:
                        signalToPotreeConverterMPI.write("\nnotlastminibatchinpartition")
                if (self.lazFilestoProcess == []):
                    signalToPotreeConverterMPI.write("\nlastbatch")
                else:
                    signalToPotreeConverterMPI.write("\nnotlastbatch")
            open(signalDir + "/" + "batchno_" + str(batchCopier.getNumBatchesCopied() - 1) + "_written", "w").close()

    def distribution(self, lazPartition, partitionId):
        ''' Thev function to load/unload partitions for the distribution phase of the PotreeConverterMPI program
        :param lazPartition
        :param partition number
        '''
        self.lazDistributionBatch = lazPartition.copy()
        while self.lazDistributionBatch:  # loop until all files are copied
            size = 0
            filesToCopy = []
            currlazFilesforDist = self.lazDistributionBatch.copy()
            for laz in currlazFilesforDist:
                p = Path(laz)
                size += p.stat().st_size
                if size <= self.distributionBatchSize:
                    filesToCopy.append(laz)
                    self.lazDistributionBatch.remove(laz)
                else:
                    break
            self.copyBatch(filesToCopy, size, self.tmpInputDir, self.distributionBatchCopier, self.tmpOutputDir + "/distribution_copy_done_signals", "distribution", partitionId)
            self.testBatchDone(self.distributionBatchCopier, self.tmpOutputDir + "/distribution_done_signals", "distribution")
            self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")
    def counting(self):
        ''' The function to load/unload a batch of LAZ files for the counting phase of the PotreeConverterMPI program'''

        self.logger.info("Batch copier for counting started", color="blue", bold=True)
        for partition in self.lazPartitions:
            if partition["status"] == "skip":
                continue
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

        while not self.countingBatchCopier.isbatchDictEmpty():
            self.testBatchDone(self.countingBatchCopier, self.tmpOutputDir + "/counting_done_signals", "counting")
            time.sleep(60)

        self.logger.info("Counting finished. Total " + str(
            self.numFilesDone["counting"]) + " files counted in " + str(
            len(self.batchesDone["counting"])) + " batches.", color="green", bold=True)
        self.lazFilestoProcess.clear()

    def indexing(self):
        '''The function to load/unload a batch of LAZ files for the indexing phase of the PotreeConverterMPI program'''
        self.logger.info("Batch copier for indexing started", color="blue", bold=True)
        self.lazFilestoProcess = self.lazPartitions.copy()
        self.lazFilestoProcess.sort(key=lambda x: x["size"])
        skipPartitions = []
        indexingDone = []
        numFilesdone = 0
        numBatches = 0
        totalBatches = len(self.lazFilestoProcess)
        while self.lazFilestoProcess:  # loop until all files are copied
            #if self.indexingBatchCopier.gettotalSize() < self.maxTmpSpaceAvailable:
            currbatches = self.lazFilestoProcess.copy()
            for lazBatch in currbatches:
                if lazBatch["status"] == "skip":
                    skipPartitions.append(lazBatch)
                    self.lazFilestoProcess.remove(lazBatch)
                    self.logger.warning("Skipping partition " + str(lazBatch["id"]) + ". Too big for max tmp space allowed, size: " + str(lazBatch["size"]) + " bytes, Tmp space available: " + str(self.maxTmpSpaceAvailable) + " bytes, Tmp space requrired: + " + str((self.lazCompressionRatio + 2 + 1) * lazBatch["size"]) +" bytes")
                    continue
                copiedBatchNums = self.indexingBatchCopier.getCopiedBatchNums()
                if not copiedBatchNums:
                    spaceUtilization = lazBatch["size"] * (self.lazCompressionRatio + 2 + 1)
                elif len(copiedBatchNums) == 1:
                    currBatchSize = self.indexingBatchCopier.gettotalSize()
                    #Assuming that the currBatch is immediately removed before distributiion phase of nextBatch get start
                    spaceUtilization = (currBatchSize * 2) + (lazBatch["size"] * 2) + (lazBatch["size"] * self.lazCompressionRatio)
                else:
                    currBatchSize = self.indexingBatchCopier.getBatchSize(heapq.nsmallest(2, copiedBatchNums)[0])
                    nextBatchSize = self.indexingBatchCopier.getBatchSize(heapq.nsmallest(2, copiedBatchNums)[1])
                    #Assuming that the currBatch is immediately removed before distributiion phase of nextBatch get start
                    spaceUtilization = (currBatchSize * 2) + (nextBatchSize * (self.lazCompressionRatio + 2)) + lazBatch["size"] + self.indexingBatchCopier.gettotalSize()

                # if not self.indexingBatchCopier.getCopiedBatchNums():
                #     nextBatchSize = 0
                #
                # else:
                #     nextBatchSize = self.indexingBatchCopier.getBatchSize(min(self.indexingBatchCopier.getCopiedBatchNums()))
                #if ((nextBatchSize*self.lazCompressionRatio) + (self.indexingBatchCopier.gettotalSize() - nextBatchSize + lazBatch["size"])) <= self.maxTmpSpaceAvailable:
                if spaceUtilization <= self.maxTmpSpaceAvailable:
                    batchToCopy = lazBatch.copy()
                    self.lazFilestoProcess.remove(lazBatch)

                    self.copyBatch([], batchToCopy["size"], self.tmpInputDir, self.indexingBatchCopier,
                      self.tmpOutputDir + "", "indexing", batchToCopy["id"])
                    self.distribution(batchToCopy["files"], batchToCopy["id"])
                self.testBatchDone(self.distributionBatchCopier, self.tmpOutputDir + "/distribution_done_signals", "distribution")
                self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")

        while (not self.indexingBatchCopier.isbatchDictEmpty()) or (not self.distributionBatchCopier.isbatchDictEmpty()):
            if not self.distributionBatchCopier.isbatchDictEmpty():
                self.testBatchDone(self.distributionBatchCopier, self.tmpOutputDir + "/distribution_done_signals", "distribution")
            if not self.indexingBatchCopier.isbatchDictEmpty():
                self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")
            time.sleep(60)
        self.logger.info("Indexing finished. Total " + str(
            self.numFilesDone["indexing"]) + " files indexed in " + str(
            len(self.batchesDone["indexing"])) + " partitions. Total " + str(totalBatches) + " partitions, " + str(
            len(skipPartitions)) +  " skipped" , color="green", bold=True)


    def parseBladnr(self, bladnr):
       ''' This function is used to parse the bladnr from the partitions CSV file. The following function is used to parse the bladnr for two possible formats of the bladnr:
       1. 1234
       2. /path/to/C_1234.LAZ
       Modify the following function if the bladnr format is different
       :param bladnr: The bladnr to parse
       :return: The path to the bladnr file in the input directory
       '''
       if bladnr.upper().endswith(".LAZ"):
            bladnrPath = self.InputDir + "/" + bladnr.split("/")[-1]
       else:
            bladnrPath = self.InputDir + "/C_" + bladnr.upper() + ".LAZ"
       return bladnrPath


    def createPartitions(self):
        ''' This function is used to create the partitions from the partitions CSV file. The partitions are created based on the size of the LAZ files and the max tmp space available. The partitions are stored in the lazPartitions list'''
        self.logger.info("Creating partitions...", color="blue", bold=True)
        lazfileStats = {}
        with open(self.partitionsCSV, "r", newline='') as partitionFile:
            partition = csv.reader(partitionFile)
            colName = next(partition)
            bladnrIdx = colName.index("bladnr")
            partitionIdIdx = colName.index("partition_id")
            for row in partition:
                if self.partitionsToProcess is not None:
                    if int(row[partitionIdIdx]) not in self.partitionsToProcess:
                        continue
                if row[partitionIdIdx] not in lazfileStats:
                    lazfileStats[row[partitionIdIdx]] = {}
                    lazfileStats[row[partitionIdIdx]]["bladnr"] = [row[bladnrIdx]]
                else:
                    lazfileStats[row[partitionIdIdx]]["bladnr"].append(row[bladnrIdx])


        for id in lazfileStats:
            partitionSize = 0
            filesinPartition = {}
            filesinPartition["files"] = []
            filesinPartition["id"] = id
            for bladnr in lazfileStats[id]["bladnr"]:
                bladnrPath = self.parseBladnr(bladnr)
                filesinPartition["files"].append(bladnrPath)
                partitionSize += Path(bladnrPath).stat().st_size
            filesinPartition["size"] = partitionSize
            if (self.lazCompressionRatio + 2 + 1) * partitionSize <= self.maxTmpSpaceAvailable:
                filesinPartition["status"] = "do"
            else:
                filesinPartition["status"] = "skip"
                self.logger.warning(f"Partition {id} too big for max tmp space allowed. It will be skipped. Size: {str(partitionSize)} bytes, Tmp space available: {str(self.maxTmpSpaceAvailable)} bytes, Tmp space requrired: {str((self.lazCompressionRatio + 2 + 1) * partitionSize)} bytes")

            self.lazPartitions.append(filesinPartition)
        self.logger.info("Done creating partitions. Total partitions: " + str(len(self.lazPartitions)), color="green", bold=True)


    def createDirectories(self):
        ''' This function is used to create the directories for the PotreeConverterMPI program. It also copies the headers to the temporary directory'''
        self.logger.info("Creating directories...", color="blue", bold=True)
        if not Path(self.InputDir).exists():
            self.logger.error("Input directory " + self.InputDir + " does not exist")
            exit(1)
        elif not Path(self.lazHeadersToCopy).exists():
            self.logger.error("Headers directory " + self.lazHeadersToCopy + " does not exist")
            exit(1)


        if Path(self.tmpDir).exists():
            print("Directory "  + self.tmpDir  + " for temporarily storing partial input and output already exists. Do you want to overwrite it? (y/n)")
            answer = input()
            if answer == "y" or answer == "Y":
                self.logger.info("Removing directory " + self.tmpDir + " for temporarily storing partial input and output...")
                shutil.rmtree(self.tmpDir)
            else:
                self.logger.info("Directory " + self.tmpDir + " for temporarily storing partial input and output already exists. Exiting on user request... ")
                exit(1)
        self.logger.info("Creating directory " + self.tmpDir + " for temporarily storing partial input and output...")
        Path(self.tmpDir).mkdir()
        if self.tmpInputDir != self.InputDir:
            Path(self.tmpInputDir).mkdir()
        Path(self.tmpOutputDir).mkdir()

        if Path(self.OutputDir).exists():
            print("Output directory " + self.OutputDir + " already exists. Do you want to overwrite it? (y/n)")
            answer = input()
            if answer == "y" or answer == "Y":
                self.logger.info("Removing output directory " + self.OutputDir + "...")
                shutil.rmtree(self.OutputDir)
            else:
                self.logger.info(
                    "Output directory " + self.OutputDir + " already exists. Exiting on user request... ")
                exit(1)
        self.logger.info("Creating output directory " + self.OutputDir + "...")
        Path(self.OutputDir).mkdir()
        self.logger.info("Creating directory " + self.lazHeadersDir + " for temporarily storing headers...")
        Path(self.lazHeadersDir).mkdir()
        lazHeaders = []
        for entry in self.lazPartitions:
            lazHeaders.extend(map(lambda x: self.lazHeadersToCopy + "/" + Path(x).stem + ".json", entry["files"]))
        self.logger.info("Copying headers...", color="blue", bold=True)
        self.miscCopier.copyFiles(lazHeaders, self.lazHeadersDir)
        pathsCount = 0
        for path in glob.glob(self.lazHeadersDir + "/*.json"):
            if not glob.glob(self.InputDir + "/" + Path(path).stem + ".[lL][aA][zZ]"):
                self.logger.error("LAZ file for " + Path(path).name + " in the input directory " + self.InputDir + " does not exist")
                exit(1)
            pathsCount += 1
        if pathsCount == 0:
            self.logger.error("No LAZ files to process in the input directory")
            exit(1)
        #self.miscCopier.copyFiles(glob.glob(self.lazHeadersToCopy + "/*.json"), self.lazHeadersDir)
        self.logger.info("Done copying headers", color="green", bold=True)
        Path(self.tmpOutputDir + "/counting_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/indexing_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/distribution_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/counting_done_signals").mkdir()
        Path(self.tmpOutputDir + "/indexing_done_signals").mkdir()
        Path(self.tmpOutputDir + "/distribution_done_signals").mkdir()
        self.logger.info("Done creating directories", color="green", bold=True)

    def removeDirectories(self, directories):
        ''' This function is used to remove the directories'''
        for directory in directories:
            shutil.rmtree(directory)


def PotreeConverterMPIBatchedCopier(potreeConverterBatched):
    potreeConverterBatched.counting()
    potreeConverterBatched.indexing()

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
