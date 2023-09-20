import shutil
import subprocess
import time
from pathlib import Path
from copier import Copier

NAME_MAX = 255
PATH_MAX = 4096
class LocalCopier(Copier):

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



    def filesSubsets(self, filesList, NAME_MAX = 255, PATH_MAX = 4096):
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
                if subsetNameLength + currFilesNamesLength[i]  <= NAME_MAX and subsetPathLength + currFilesPathLength[i]  <= PATH_MAX:
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
        subsets = self.filesSubsets(filesToRemove, NAME_MAX, PATH_MAX)
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
            startCopyTime = time.time()
            self.copyFiles(filesToCopy, destination)
            copyTime = time.time() - startCopyTime
            self.totalCopyTime += copyTime
            self.totalSize += self.storageUtilizationFactor * size
            self.logger.info("Copied batch-" + str(self.batchCopied) + "/partition-" + partition +  " to " + destination + ", size: "  + str(size/(1024**3)) + " gigabytes, " + "time: " + str(copyTime) + ", copying throughput: " + str((size / copyTime) / (1024**2)) + " MB/s")#: " + ",".join(map(lambda x: Path(x).name, filesToCopy.split())))
        self.batchCopied += 1
    def copyFiles(self, filesToCopy, destination):
        subsets = self.filesSubsets(filesToCopy, NAME_MAX, PATH_MAX)
        for subset in subsets:
            copyCmd = shutil.which("cp") + " " + " ".join(subset) + " " + destination
            copyCmdStatus = subprocess.run(copyCmd, shell=True, capture_output=True, encoding="utf-8")
            if copyCmdStatus.returncode != 0:
                self.logger.error("Error copying files: " + copyCmdStatus.stderr)
                exit(1)

    def concatFiles(self, filesToConcat, destination):
        concatCmd = shutil.which("cat") + " " + " ".join(filesToConcat) +  " >> " + destination
        startConcatTime = time.time()
        concatCmdStatus = subprocess.run(concatCmd, shell=True, capture_output=True, encoding="utf-8")
        concatTime = time.time() - startConcatTime
        size = sum(map(lambda x: Path(x).stat().st_size, filesToConcat))
        if concatCmdStatus.returncode != 0:
            self.logger.error("Error concatenating files: " + concatCmdStatus.stderr)
            exit(1)
        self.logger.info("Files: " + ",".join(filesToConcat) + " concatenated to " + destination + ", size:" + str(size/(1024**3)) + " gigabytes, time: " + str(concatTime) + ", concatenating throughput: " + str((size / concatTime) / (1024**2)) + " MB/s")

    def getPartitionNum(self, batchNum):
        return self.batchDict[batchNum]["partition"]
    def getFiles(self, batchNum):
        return self.batchDict[batchNum]["files"]