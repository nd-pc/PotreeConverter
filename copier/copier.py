import shutil
import subprocess
import time
from pathlib import Path



class Copier:


    def __init__(self, log, factor = 1):
        self.batchCopied = 0
        self.batchDict = {}
        self.totalSize = 0
        self.storageUtilizationFactor = factor
        self.totalCopyTime = 0
        self.logger = log
        self.NAME_MAX = 255
        self.PATH_MAX = 4096

    def removeBatch(self, batchNum):
        if batchNum in self.batchDict:
            if self.batchDict[batchNum]["files"] != []:
                self.removeFiles(self.batchDict[batchNum]["files"])
                self.logger.info("Removed batch-" + str(batchNum) + "/partition-" + self.batchDict[batchNum]["partition"] + " from " + self.batchDict[batchNum]["destdir"])
                size = self.batchDict[batchNum]["size"]
                self.totalSize -= self.storageUtilizationFactor * size #update the total size
            del self.batchDict[batchNum]
        else:
            self.logger.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)

            #remove the batch from the dictionary

    def filesSubsets(self, filesList):
        filesToSubset = filesList.copy()
        filesNamesLength = list(map(lambda x: len(Path(x).name), filesToSubset))
        filesPathLength = list(map(lambda x: len(x), filesToSubset))
        subsets = []
        while filesToSubset:
            subset = []
            subsetNameLength = 0
            subsetPathLength = 0
            currFilesList = filesToSubset.copy()
            currFilesNamesLength = filesNamesLength.copy()
            currFilesPathLength = filesPathLength.copy()
            for i in range(len(currFilesList)):
                if subsetNameLength + currFilesNamesLength[i]  <= self.NAME_MAX and subsetPathLength + currFilesPathLength[i]  <= self.PATH_MAX:
                    subset.append(currFilesList[i])
                    subsetNameLength += currFilesNamesLength[i] + 1
                    subsetPathLength += currFilesPathLength[i] + 1
                    filesToSubset.pop(0)
                    filesNamesLength.pop(0)
                    filesPathLength.pop(0)
                else:
                    break
            subsets.append(subset)
        return subsets


    def removeFiles(self, filesToRemove):
        subsets = self.filesSubsets(filesToRemove)
        for subset in subsets:
            rmCmd = shutil.which("rm") + " " + " ".join(subset)
            rmCmdStatus = subprocess.run(rmCmd, shell=True, capture_output=True, encoding="utf-8")
            if rmCmdStatus.returncode != 0:
                self.logger.error("Error removing file: " + rmCmdStatus.stderr)
                exit(1)


    def copyBatch(self, filesToCopy, size, destination, partition = ""):
        if partition == "":
            partition = str(self.batchCopied)
        self.batchDict[self.batchCopied] = {"files": list(map(lambda x: str(destination + "/" + Path(x).name), filesToCopy)), "size": size, "starttime":time.time(), "destdir": destination, "partition": partition}
        if filesToCopy != []:
            self.logger.info("Copying batch-" + str(self.batchCopied) + "/partition-" + partition + " to " + destination + ", size: " + str(size/(1024**3)) + " gigabytes ...")
            startCopyTime = time.time()
            self.copyFiles(filesToCopy, destination)
            copyTime = time.time() - startCopyTime
            self.totalCopyTime += copyTime
            self.totalSize += self.storageUtilizationFactor * size
            self.logger.info("Copying finished for batch-" + str(self.batchCopied) + "/partition-" + partition +  " to " + destination + ", size: "  + str(size/(1024**3)) + " gigabytes, " + "time: " + str(copyTime) + ", copying throughput: " + str((size / copyTime) / (1024**2)) + " MB/s", color="green")
        self.batchCopied += 1
    def concatFiles(self, filesToConcat, destination):
        self.logger.info("Concatenating files: " + ",".join(filesToConcat) + " to " + destination + " ...")
        concatCmd = shutil.which("cat") + " " + " ".join(filesToConcat) +  " >> " + destination
        startConcatTime = time.time()
        concatCmdStatus = subprocess.run(concatCmd, shell=True, capture_output=True, encoding="utf-8")
        concatTime = time.time() - startConcatTime
        size = sum(map(lambda x: Path(x).stat().st_size, filesToConcat))
        if concatCmdStatus.returncode != 0:
            self.logger.error("Error concatenating files: " + concatCmdStatus.stderr)
            exit(1)
        self.logger.info("Files: " + ",".join(filesToConcat) + " concatenated to " + destination + ", size:" + str(size/(1024**3)) + " gigabytes, time: " + str(concatTime) + ", concatenating throughput: " + str((size / concatTime) / (1024**2)) + " MB/s")

    def copyFiles(self, filesToCopy, destination):
        raise NotImplementedError("copyFiles method is implemented in subclasses")
    def getNumBatchesCopied(self):
        return self.batchCopied

    def gettotalSize(self):
        return self.totalSize
    def setTotalSize(self, size):
        self.totalSize = size
    def getCopiedBatchNums(self):
        return self.batchDict.keys()
    def isbatchDictEmpty(self):
        if len(self.batchDict) == 0:
            return True
        else:
            return False
    def getTotalCopyTime(self):
        return self.totalCopyTime

    def getPartitionNum(self, batchNum):
        if batchNum not in self.batchDict:
            self.logger.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["partition"]
    def getFiles(self, batchNum):
        if batchNum not in self.batchDict:
            self.logger.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["files"]

    def getBatchSize(self, batchNum):
        if batchNum not in self.batchDict:
            self.logger.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["size"]
    def getMaxBatchSize(self):
        if len(self.batchDict) == 0:
            return 0 #if there are no batches, return 0
        return max(map(lambda x: self.batchDict[x]["size"], self.batchDict))