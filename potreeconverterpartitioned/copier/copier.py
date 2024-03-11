import shutil
import subprocess
import time
from pathlib import Path

from potreeconverterpartitioned.loggingwrapper import LoggingWrapper

class Copier:

    '''Copier classs is  used forcopying files from one location to another.
    It copyies the files in batches.
    The copier class keeps track of the batches that are copied and the total size of the files that are copied.'''

    def __init__(self):
        self.batchCopied = 0
        self.batchDict = {}
        self.totalSize = 0
        self.totalCopyTime = 0
        self.NAME_MAX = 255
        self.PATH_MAX = 4096

    def removeBatch(self, batchNum):
        '''Removes the batch of files from the destination directory
        :param batchNum: the batch number to remove'''
        if batchNum in self.batchDict:
            if self.batchDict[batchNum]["files"] != []:
                self.removeFiles(self.batchDict[batchNum]["files"])
                LoggingWrapper.info("Removed batch-" + str(batchNum) + "/partition-" + self.batchDict[batchNum]["partition"] + " from " + self.batchDict[batchNum]["destdir"])
                size = self.batchDict[batchNum]["size"]
                self.totalSize -= size #update the total size
            del self.batchDict[batchNum]
        else:
            LoggingWrapper.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)

            #remove the batch from the dictionary

    def filesSubsets(self, filesList):
        '''Splits the files into subsets so that the length of the file names and the length of the file paths are less than the maximum allowed
        :param filesList: the list of files to split
        :return: the list of subsets'''
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
        '''Removes the files from the destination directory
        :param filesToRemove: the list of files to remove'''
        subsets = self.filesSubsets(filesToRemove)
        for subset in subsets:
            rmCmd = shutil.which("rm") + " " + " ".join(subset)
            rmCmdStatus = subprocess.run(rmCmd, shell=True, capture_output=True, encoding="utf-8")
            if rmCmdStatus.returncode != 0:
                LoggingWrapper.error("Error removing file: " + rmCmdStatus.stderr)
                exit(1)


    def copyBatch(self, filesToCopy, size, destination, partition = ""):
        '''Copies the batch of files to the destination directory
        :param filesToCopy: the list of files to copy
        :param size: the total size of the files to copy
        :param destination: the destination directory
        :param partition: the partition number of the batch'''
        if partition == "":
            partition = str(self.batchCopied)
        self.batchDict[self.batchCopied] = {"files": list(map(lambda x: str(destination + "/" + Path(x).name), filesToCopy)), "size": size, "starttime":time.time(), "destdir": destination, "partition": partition}
        if filesToCopy != []:
            LoggingWrapper.info("Copying batch-" + str(self.batchCopied) + "/partition-" + partition + " to " + destination + ", size: " + str(size/(1024**3)) + " gigabytes ...")
            startCopyTime = time.time()
            self.copyFiles(filesToCopy, destination)
            copyTime = time.time() - startCopyTime
            self.totalCopyTime += copyTime
            self.totalSize += size
            LoggingWrapper.info("Copying finished for batch-" + str(self.batchCopied) + "/partition-" + partition +  " to " + destination + ", size: "  + str(size/(1024**3)) + " gigabytes, " + "time: " + str(copyTime) + ", copying throughput: " + str((size / copyTime) / (1024**2)) + " MB/s", color="green")
        self.batchCopied += 1
    def concatFiles(self, filesToConcat, destination):
        """Concatenates the files to the destination file
        :param filesToConcat: the list of files to concatenate
        :param destination: the destination file
        """
        LoggingWrapper.info("Concatenating files: " + ",".join(filesToConcat) + " to " + destination + " ...")
        concatCmd = shutil.which("cat") + " " + " ".join(filesToConcat) +  " >> " + destination
        startConcatTime = time.time()
        concatCmdStatus = subprocess.run(concatCmd, shell=True, capture_output=True, encoding="utf-8")
        concatTime = time.time() - startConcatTime
        size = sum(map(lambda x: Path(x).stat().st_size, filesToConcat))
        if concatCmdStatus.returncode != 0:
            LoggingWrapper.error("Error concatenating files: " + concatCmdStatus.stderr)
            exit(1)
        LoggingWrapper.info("Files: " + ",".join(filesToConcat) + " concatenated to " + destination + ", size:" + str(size/(1024**3)) + " gigabytes, time: " + str(concatTime) + ", concatenating throughput: " + str((size / concatTime) / (1024**2)) + " MB/s")

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
        """Returns the partition number of the batch
        :param batchNum: the batch number
        :return: the partition number of the batch"""
        if batchNum not in self.batchDict:
            LoggingWrapper.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["partition"]
    def getFiles(self, batchNum):
        """Returns the files of the batch
        :param batchNum: the batch number
        :return: the files of the batch"""

        if batchNum not in self.batchDict:
            LoggingWrapper.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["files"]

    def getBatchSize(self, batchNum):
        """Returns the size of the batch
        :param batchNum: the batch number
        :return: the size of the batch"""
        if batchNum not in self.batchDict:
            LoggingWrapper.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["size"]
    def getMaxBatchSize(self):
        """Returns the size of the largest batch
        :return: the size of the largest batch"""
        if len(self.batchDict) == 0:
            return 0 #if there are no batches, return 0
        return max(map(lambda x: self.batchDict[x]["size"], self.batchDict))