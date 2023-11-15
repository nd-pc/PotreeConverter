
from loggingwrapper import LoggingWrapper
class Copier:

    def __init__(self, factor = 1):
        self.batchCopied = 0
        self.batchDict = {}
        self.totalSize = 0
        self.storageUtilizationFactor = factor
        self.totalCopyTime = 0



    def removeBatch(self, batchNum):
        raise NotImplementedError("Subclass must implement abstract method")
    def copyBatch(self, filesToCopy, size, destination, partition = ""):
        raise NotImplementedError("Subclass must implement abstract method")
    def concatFiles(self, filesToConcat, destination):
        raise NotImplementedError("Subclass must implement abstract method")
    def copyFiles(self, filesToCopy, destination):
        raise NotImplementedError("Subclass must implement abstract method")
    def removeFiles(self, filesToRemove):
        raise NotImplementedError("Subclass must implement abstract method")

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
            LoggingWrapper.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["partition"]
    def getFiles(self, batchNum):
        if batchNum not in self.batchDict:
            LoggingWrapper.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["files"]

    def getBatchSize(self, batchNum):
        if batchNum not in self.batchDict:
            LoggingWrapper.error("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        return self.batchDict[batchNum]["size"]
    def getMaxBatchSize(self):
        if len(self.batchDict) == 0:
            return 0 #if there are no batches, return 0
        return max(map(lambda x: self.batchDict[x]["size"], self.batchDict))