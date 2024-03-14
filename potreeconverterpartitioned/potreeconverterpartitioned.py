
import configparser
import csv
import glob
import heapq
import shutil
import time

from pathlib import Path

from .copier import ParallelCopier
from .loggingwrapper import LoggingWrapper
from .scheduler import SbatchScheduler
from .scheduler import LocalScheduler
from .scheduler import QsubScheduler


class PotreeConverterBatched:
    ''' A driver for the PotreeConverterMPI program. This starts the PotreeConverterMPI program and loads/unloads the partitions. '''
    def __init__(self, configFile):
        ''' Reads the config file and initializes the class variables. Also starts the PotreeConverterMPI program. '''
        #class variables
        self.maxTmpSpaceAvailable = 0
        self.countingBatchSize = 0
        self.distributionBatchSize = 0
        self.lazCompressionRatio = 0
        self.inputToOutputSizeRatio = 0
        self.inputToTmpInputSizeRatio = 1
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
        self.distributionBatchCopier = None
        self.indexingBatchCopier = None
        self.miscCopier = None
        self.batchesDone = {"counting": [], "indexing": [], "distribution": []}
        self.numFilesDone = {"counting": 0, "indexing": 0, "distribution": 0}
        self.lazFilestoProcess = []
        self.lazDistributionBatch = []
        self.programCommand = ""
        self.programName = ""
        self.partitionsToProcess = None
        self.lazPartitions = []
        
        # Parse the configuration file.
        config = configparser.ConfigParser()
        config.read(configFile)
        sections = config.sections()

        # Input and output parameters.
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


            if "LazHeadersDir" not in config["INPUT_OUTPUT"]:
                LoggingWrapper.error("LazHeadersDir not found in INPUT_OUTPUT section")
                exit(1)
            else:
                self.lazHeadersToCopy = config["INPUT_OUTPUT"]["LazHeadersDir"]
        
        # Partition parameters.
        if "PARTITIONS" not in sections:
            LoggingWrapper.error("PARTITIONS section not found in config file")
            exit(1)
        else:
            if "CSV" not in config["PARTITIONS"]:
                LoggingWrapper.error("CSV not found in PARTITIONS section")
                exit(1)
            else:
                self.partitionsCSV = config["PARTITIONS"]["CSV"]

        # Temporary storage parameters.
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
                    self.tmpInputDir = self.tmpDir + "/PotreeConverterMPI_Input"
                    self.inputToTmpInputSizeRatio = 1
                
                if config["INPUT_OUTPUT"]["OutputDirType"] == "local":
                    self.tmpOutputDir = self.OutputDir
                    self.inputToOutputSizeRatio = 0
                else:
                    self.tmpOutputDir = self.tmpDir + "/PotreeConverterMPI_Output"
                    self.inputToOutputSizeRatio = 2
                
                self.lazHeadersDir = self.tmpDir + "/" + Path(self.InputDir).name + "_headers"
            if "MaxTmpSpaceAvailable" not in config["TMP_STORAGE"]:
                LoggingWrapper.error(
                    "MaxTmpSpaceAvailable not found in TMP_STORAGE section")
                exit(1)
            else:
                self.maxTmpSpaceAvailable = eval(config["TMP_STORAGE"]["MaxTmpSpaceAvailable"])
            
            if "LazCompressionRatio" not in config["TMP_STORAGE"]:
                LoggingWrapper.error(
                    "LazCompressionRatio not found in TMP_STORAGE section")
                exit(1)
            else:
                self.lazCompressionRatio = eval(config["TMP_STORAGE"]["LazCompressionRatio"])
            
            if "DistributionBatchSize" not in config["TMP_STORAGE"]:
                LoggingWrapper.error(
                    "DistributionBatchSize not found in TMP_STORAGE section")
                exit(1)
            else:
                self.distributionBatchSize = eval(config["TMP_STORAGE"]["DistributionBatchSize"])
            
            if "CountingBatchSize" not in config["TMP_STORAGE"]:
                LoggingWrapper.error(
                    "CountingBatchSize not found in TMP_STORAGE section")
                exit(1)
            else:
                self.countingBatchSize = eval(config["TMP_STORAGE"]["CountingBatchSize"])

        # Copier parameters.
        if "COPIER" not in sections:
            LoggingWrapper.error("COPIER section not found in config file")
            exit(1)

        else:
            if "Type" not in config["COPIER"]:
                LoggingWrapper.error("Type not found in COPIER section")
                exit(1)
            else:
                self.copierType = config["COPIER"]["Type"]
        if self.copierType == "cp":
            self.countingBatchCopier = ParallelCopier()
            self.distributionBatchCopier = ParallelCopier()
            self.indexingBatchCopier = ParallelCopier()
            self.miscCopier = ParallelCopier()
        else:
            LoggingWrapper.error("Copier type not recognized. Must be only local")
            exit(1)

        # Create the partitions and required directories.
        self.createPartitions()
        self.createDirectories()

        # Executable parameters.
        if "PROGRAM" not in sections:
            LoggingWrapper.error("PROGRAM section not found in config file")
            exit(1)
        else:
            if "Path" not in config["PROGRAM"]:
                LoggingWrapper.error("Program path not found in PROGRAM section")
                exit(1)
            else:
                self.programPath = config["PROGRAM"]["Path"]
                self.programName = config["PROGRAM"]["Path"].split("/")[-1]
            
            if "Options" in config["PROGRAM"]:
                self.programOptions = config["PROGRAM"]["Options"]

        # Scheduler parameters.
        if "SCHEDULER" in sections:
            if "Parameters" not in config["SCHEDULER"]:
                LoggingWrapper.error("Parameters not found in SCHEDULER section")
                exit(1)
            if "Type" in config["SCHEDULER"]:
                if config["SCHEDULER"]["Type"] == "slurm":
                    # SLURM batch script.
                    parameters = config["SCHEDULER"]["Parameters"]
                    with open(self.tmpDir + "/sbatchScript.sh", "w") as sbatchScript:
                        sbatchScript.write("#!/bin/sh\n\n")
                        sbatchScript.write("#SBATCH " + parameters + "\n\n")
                        sbatchScript.write(shutil.which("mpiexec") + " " + self.programPath +  " " + self.programOptions + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir)
                    programCommand = shutil.which("sbatch") + " " + self.tmpDir + "/sbatchScript.sh"
                    self.scheduler = SbatchScheduler(programCommand, self.programName)
                elif config["SCHEDULER"]["Type"] == "pbs":
                    # PBS script.
                    parameters = config["SCHEDULER"]["Parameters"]
                    with open(self.tmpDir + "/qsubScript.sh", "w") as qsubScript:
                        qsubScript.write("#!/bin/sh\n\n")
                        qsubScript.write("#PBS " + parameters + "\n\n")
                        qsubScript.write(shutil.which("mpiexec") + " " +self.programPath + " " + self.programOptions + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir)
                    programCommand = shutil.which("qsub") + " " + self.tmpDir + "/qsubScript.sh"
                    self.scheduler = QsubScheduler(programCommand, self.programName)
                elif config["SCHEDULER"]["Type"] == "local":
                    # Local execution.
                    programCommand = self.programPath + " " + self.programOptions + " -o " + self.tmpOutputDir + " --header-dir " + self.lazHeadersDir
                    self.scheduler = LocalScheduler(programCommand, self.programName)
                else:
                    LoggingWrapper.error("Scheduler type not recognized. Must be one of sbatch, pbs or local")
                    exit(1)
            else:
                LoggingWrapper.error("Scheduler type not found in SCHEDULER section")
                exit(1)
        else:
            LoggingWrapper.error("SCHEDULER section not found in config file")
            exit(1)


    def bordered(self, text):
        ''' Prints bordered text. '''
        lines = text.split("\n")
        width = max(len(s) for s in lines)
        res = ['=' * width]
        for s in lines:
            res.append('||' + (s + ' ' * width)[:width] + '||')
        res.append('=' * width)
        return '\n'.join(res)

    def catFiles(self, filesToCat, destinationFile, batchCopier):
        '''
        Concatenates the output files of the PotreeConverterMPI program after processing a partition.
        
        Parameters
        ----------
        filesToCat : str
            The list of files to concatenate
        destinationFile : str
            The destination file
        batchCopier : obj
            The copier object
        '''

        batchCopier.concatFiles(filesToCat, destinationFile)
        batchCopier.removeFiles(filesToCat)


    def testBatchDone(self, batchCopier, signalDir, state):
        '''
        Tests if a batch/partition is done processing.

        If a batch is done processing, it will be removed from the batch dictionary and the corresponding signal files will be removed.
        
        Parameters
        ----------
        batchCopier : obj
            The copier object
        signalDir : str
            The directory where to read the signal files from the PotreeConverterMPI program
        state : str
            The state of the processing (counting, indexing or distribution)
        '''
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
                LoggingWrapper.info(partitionorbatch + "-" + str(
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
            
            LoggingWrapper.info(state.capitalize() +  " pending for " + partitionorbatch.lower() + "s: " + pending + ". Done " + partitionorbatch.lower() + "s: " + done)

    
    def copyBatch(self, filesToCopy, size, destination, batchCopier, signalDir, state, partition=""):
        '''
        Copies a batch/partition to the temporary input directory.

        Parameters
        ----------
        filesToCopy : str
            The list of files to copy
        size : int
            The total size of the files to copy
        destination : str
            The destination directory
        batchCopier : obj
            The copier object
        signalDir : str
            The directory where to write signal files for the PotreeConverterMPI program
        state : str
            The state of the batch/partition
        partition : str
            The partition number
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
        '''
        Loads/unloads partitions for the distribution phase of the PotreeConverterMPI program.

        Parameters
        ----------
        lazPartition : str
            LAZ partition files
        partitionId : str
            partition number
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
        ''' Loads/unloads a batch of LAZ files for the counting phase. '''

        LoggingWrapper.info("==================================================")
        LoggingWrapper.info("Batch copier for counting started", color="blue", bold=True)

        for partition in self.lazPartitions:
            if partition["status"] == "skip":
                continue
            self.lazFilestoProcess.extend(partition["files"])

        # Copy all files and test whether the batch is done.
        while self.lazFilestoProcess:
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

        # Wait until all batches are done.
        while not self.countingBatchCopier.isbatchDictEmpty():
            self.testBatchDone(self.countingBatchCopier, self.tmpOutputDir + "/counting_done_signals", "counting")
            time.sleep(60)

        LoggingWrapper.info("Counting finished", color="green", bold=True)
        LoggingWrapper.info("==================================================")


    def indexing(self):
        ''' Loads/unloads a batch of LAZ files for the distribution and indexing phase. '''
        
        LoggingWrapper.info("==================================================")
        LoggingWrapper.info("Batch copier for distribution and indexing started", color="blue", bold=True)
        self.lazFilestoProcess = self.lazPartitions.copy()
        self.lazFilestoProcess.sort(key=lambda x: x["size"])
        skipPartitions = []
        totalBatches = len(self.lazFilestoProcess)

        # Copy all files and test whether the batch is done.
        while self.lazFilestoProcess:
            currbatches = self.lazFilestoProcess.copy()
            for lazBatch in currbatches:
                if lazBatch["status"] == "skip":
                    skipPartitions.append(lazBatch)
                    self.lazFilestoProcess.remove(lazBatch)
                    LoggingWrapper.warning("Skipping partition " + str(lazBatch["id"]))
                    continue
            
                copiedBatchNums = self.indexingBatchCopier.getCopiedBatchNums()
                if not copiedBatchNums: # if no batch is waiting to be processed
                    spaceUtilization = lazBatch["size"] * (self.lazCompressionRatio + self.inputToOutputSizeRatio + self.inputToTmpInputSizeRatio)
                elif len(copiedBatchNums) == 1: # if a batch is already being indexed
                    currBatchSize = self.indexingBatchCopier.gettotalSize()
                    #Assuming that the currBatch is immediately removed before distributiion phase of nextBatch get start
                    spaceUtilization = (currBatchSize * self.inputToOutputSizeRatio) + (lazBatch["size"] * self.inputToOutputSizeRatio) + (lazBatch["size"] * self.lazCompressionRatio)
                else: #if a batch is already being indexed and others are waiting
                    currBatchSize = self.indexingBatchCopier.getBatchSize(heapq.nsmallest(2, copiedBatchNums)[0])
                    nextBatchSize = self.indexingBatchCopier.getBatchSize(heapq.nsmallest(2, copiedBatchNums)[1])
                    #Assuming that the currBatch is immediately removed before distributiion phase of nextBatch get start
                    spaceUtilization = (currBatchSize * self.inputToOutputSizeRatio) + (nextBatchSize * (self.lazCompressionRatio + self.inputToOutputSizeRatio)) + lazBatch["size"] + self.indexingBatchCopier.gettotalSize()

                if spaceUtilization <= self.maxTmpSpaceAvailable:
                    batchToCopy = lazBatch.copy()
                    self.lazFilestoProcess.remove(lazBatch)
                    self.distribution(batchToCopy["files"], batchToCopy["id"])
                    self.copyBatch([], batchToCopy["size"], self.tmpInputDir, self.indexingBatchCopier,
                                   self.tmpOutputDir + "", "indexing", batchToCopy["id"])
                
                self.testBatchDone(self.distributionBatchCopier, self.tmpOutputDir + "/distribution_done_signals", "distribution")
                self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")

        # Wait until all batches are done.
        while (not self.indexingBatchCopier.isbatchDictEmpty()) or (not self.distributionBatchCopier.isbatchDictEmpty()):
            if not self.distributionBatchCopier.isbatchDictEmpty():
                self.testBatchDone(self.distributionBatchCopier, self.tmpOutputDir + "/distribution_done_signals", "distribution")
            if not self.indexingBatchCopier.isbatchDictEmpty():
                self.testBatchDone(self.indexingBatchCopier, self.tmpOutputDir + "/indexing_done_signals", "indexing")
            time.sleep(60)
        
        LoggingWrapper.info("Indexing finished", color="green", bold=True)
        LoggingWrapper.info("==================================================")


    def parseBladnr(self, bladnr):
        '''
        Parses the "bladnr" from the partitions CSV file.
        
        The following function is used to parse the bladnr for two possible formats of the bladnr:
        1. 1234
        2. /path/to/C_1234.LAZ
        
        Modify the following function if the bladnr format is different.

        Parameters
        ----------
        bladnr: str
            The bladnr to parse.

        Returns
        ----------
        str
            The path to the bladnr file in the input directory.
        '''

        if bladnr.upper().endswith(".LAZ"):
             bladnrPath = self.InputDir + "/" + bladnr.split("/")[-1]
        else:
             bladnrPath = self.InputDir + "/C_" + bladnr.upper() + ".LAZ"
        return bladnrPath


    def createPartitions(self):
        '''
        Creates the partitions from the partitions CSV file.
        
        The partitions are created based on the size of the LAZ files and the max tmp space available.
        The partitions are stored in the lazPartitions list.
        '''
        
        LoggingWrapper.info("Creating partitions...", color="blue", bold=True)
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
                LoggingWrapper.warning(f"Partition {id} too big for max tmp space allowed. It will be skipped. Size: {str(partitionSize)} bytes, Tmp space available: {str(self.maxTmpSpaceAvailable)} bytes, Tmp space requrired: {str((self.lazCompressionRatio + 2 + 1) * partitionSize)} bytes")

            self.lazPartitions.append(filesinPartition)
        LoggingWrapper.info("Done creating partitions. Total partitions: " + str(len(self.lazPartitions)), color="green", bold=True)
        for partition in self.lazPartitions:
            LoggingWrapper.info("partition " + partition["id"] + " (" + partition["status"] + "): " + str(partition["files"]))


    def createDirectories(self):
        '''
        Creates the directories for the PotreeConverterMPI program.
        
        Also copies the LAZ headers to the temporary directory.
        '''
        
        LoggingWrapper.info("Creating directories...", color="blue", bold=True)

        # Create input directory.
        if not Path(self.InputDir).exists():
            LoggingWrapper.error("Input directory " + self.InputDir + " does not exist")
            exit(1)
        elif not Path(self.lazHeadersToCopy).exists():
            LoggingWrapper.error("Headers directory " + self.lazHeadersToCopy + " does not exist")
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
        Path(self.tmpOutputDir).mkdir()

        # Create output directory.
        if Path(self.OutputDir).exists():
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

        # Create LAZ header directory.
        LoggingWrapper.info("Creating directory " + self.lazHeadersDir + " for temporarily storing headers...")
        Path(self.lazHeadersDir).mkdir()
        lazHeaders = []
        for entry in self.lazPartitions:
            lazHeaders.extend(map(lambda x: self.lazHeadersToCopy + "/" + Path(x).stem + ".json", entry["files"]))

        # Copy LAZ headers.
        LoggingWrapper.info("Copying headers...", color="blue", bold=True)
        self.miscCopier.copyFiles(lazHeaders, self.lazHeadersDir)
        pathsCount = 0
        for path in glob.glob(self.lazHeadersDir + "/*.json"):
            if not glob.glob(self.InputDir + "/" + Path(path).stem + ".[lL][aA][zZ]"):
                LoggingWrapper.error("LAZ file for " + Path(path).name + " in the input directory " + self.InputDir + " does not exist")
                exit(1)
            pathsCount += 1
        if pathsCount == 0:
            LoggingWrapper.error("No LAZ files to process in the input directory")
            exit(1)
        LoggingWrapper.info("Done copying headers", color="green", bold=True)

        Path(self.tmpOutputDir + "/counting_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/indexing_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/distribution_copy_done_signals").mkdir()
        Path(self.tmpOutputDir + "/counting_done_signals").mkdir()
        Path(self.tmpOutputDir + "/indexing_done_signals").mkdir()
        Path(self.tmpOutputDir + "/distribution_done_signals").mkdir()
        LoggingWrapper.info("Done creating directories", color="green", bold=True)


    def removeDirectories(self, directories):
        ''' Removes directories. '''
        
        for directory in directories:
            shutil.rmtree(directory)


