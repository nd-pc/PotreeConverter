
class Copier:

    def __init__(self, log, factor = 1):
        self.batchCopied = 0
        self.batchDict = {}
        self.totalSize = 0
        self.storageUtilizationFactor = factor
        self.totalCopyTime = 0
        self.logger = log



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
    def getCopiedBatchesKeys(self):
        return self.batchDict.keys()
    def isbatchDictEmpty(self):
        if len(self.batchDict) == 0:
            return True
        else:
            return False
    def getTotalCopyTime(self):
        return self.totalCopyTime
    def getPartitionNum(self, batchNum):
        raise NotImplementedError("Subclass must implement abstract method")
    def getFiles(self, batchNum):
        raise NotImplementedError("Subclass must implement abstract method")