

import glob
import subprocess
from datetime import datetime
from pathlib import Path
import time
import csv
import psutil
import shutil

from multiprocessing import Process, Queue

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def disable(self):
        self.HEADER = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.WARNING = ''
        self.FAIL = ''
        self.ENDC = ''

scratchSpaceAvailable = 17*(1024)**3 #

sizePerIterforCounting = 1*(1024)**3 #1 GB

spaceUtilizationFactorCounting = 1
spaceUtilizationFactorIndexing = 5

storageInputDir = "/home/anauman/staff-umbrella/ahn3_sim_data"

headersDir = "/scratch/anauman/escience/projects/nD-PC/ahn3_sim_data_headers"

scratchInputDir = "/scratch/anauman/escience/projects/nD-PC/" + Path(storageInputDir).name

scratchOutDir = scratchInputDir + "_PotreeConverterMPI_output_FP_error"

storageOutDir = storageInputDir + "_PotreeConverterMPI_output_FP_error"

AHN3partitions = "ahn3_partitions_4x4.csv"


logFile = "log.txt"

PATH_MAX = 4096

NAME_MAX = 255



class BatchCopier:

    def __init__(self, factor):
        self.batchCopied = 0
        self.batchDict = {}
        self.totalSize = 0
        self.storageUtilizationFactor = factor
        self.totalTimeExexcuting = 0
        self.totalTimeWaiting = 0
        self.totalCopyRemoveTime = 0
        self.totalCopyTime = 0


    def removeBatch(self, batchNum):
        if batchNum in self.batchDict:
            self.removeFiles(self.batchDict[batchNum]["files"])
        else:
            print("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        size = self.batchDict[batchNum]["size"]
        self.totalSize -= self.storageUtilizationFactor * size #update the total size
        print("Removed batch-" + str(batchNum) + "/partition-" + self.batchDict[batchNum]["partition"] + " from " + self.batchDict[batchNum]["destdir"])
        del self.batchDict[batchNum]    #remove the batch from the dictionary

        return size


    def filesSubsets(self, filesToSubset):
        filesList = filesToSubset.split()
        filesNamesLength = list(map(lambda x: len(Path(x).name), filesList))
        filesPathLength = list(map(lambda x: len(x), filesList))
        subsets = []
        while filesList:
            subset = ""
            subsetNameLength = 0
            subsetPathLength = 0
            currFilesList = filesList.copy()
            currFilesNamesLength = filesNamesLength.copy()
            currFilesPathLength = filesPathLength.copy()
            for i in range(len(currFilesList)):
                if subsetNameLength + currFilesNamesLength[i]  <= NAME_MAX and subsetPathLength + currFilesPathLength[i]  <= PATH_MAX:
                    subset += currFilesList[i] + " "
                    subsetNameLength += currFilesNamesLength[i] + 1
                    subsetPathLength += currFilesPathLength[i] + 1
                    filesList.pop(0)
                    filesNamesLength.pop(0)
                    filesPathLength.pop(0)
                else:
                    break
            subsets.append(subset)
        return subsets



    def removeFiles(self, filesToRemove):
        subsets = self.filesSubsets(filesToRemove)
        for subset in subsets:
            rmCmd = shutil.which("rm") + " " + subset
            rmCmdStatus = subprocess.run(rmCmd, shell=True, capture_output=True, encoding="utf-8")
            if rmCmdStatus.returncode != 0:
                print("Error removing file: " + rmCmdStatus.stderr)
                exit(1)
        #print("Removed files: " + filesToRemove)

    def copyFiles(self, filesToCopy, size,  destination, partition = ""):
        subsets = self.filesSubsets(filesToCopy)
        startCopyTime = time.time()
        for subset in subsets:
            copyCmd = shutil.which("cp") + " " + subset + " " + destination
            copyCmdStatus = subprocess.run(copyCmd, shell=True, capture_output=True, encoding="utf-8")
            if copyCmdStatus.returncode != 0:
                print("Error copying files: " + copyCmdStatus.stderr)
                exit(1)
        copyTime = time.time() - startCopyTime
        if partition == "":
            partition = str(self.batchCopied)
        self.batchDict[self.batchCopied] = {"files": " ".join(map(lambda x: str(destination + "/" + Path(x).name), filesToCopy.split())), "size": size, "starttime":time.time(), "destdir": destination, "partition": partition}
        self.totalSize += self.storageUtilizationFactor * size
        self.totalCopyRemoveTime += copyTime
        print("\tCopied batch-" + str(self.batchCopied) + "/partition-" + partition +  " to " + destination)#: " + ",".join(map(lambda x: Path(x).name, filesToCopy.split())))
        print("\tSize: " + str(size) + " bytes")
        print("\tTime: " + str(copyTime))
        print("\tCopying throughput: " + str((size / copyTime) / (1024**2)) + " MB/s")
        self.batchCopied += 1
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
    def addExecutionTime(self, time):
        self.totalTimeExexcuting += time
    def addWaitingTime(self, time):
        self.totalTimeWaiting += time
    def addCopyTime(self, time):
        self.totalCopyTime += time
    def getTotalTimeExecuting(self):
        return self.totalTimeExexcuting
    def getTotalTimeWaiting(self):
        return self.totalTimeWaiting
    def getTotalCopyTime(self):
        return self.totalCopyTime
    def getPartitionNum(self, batchNum):
        return self.batchDict[batchNum]["partition"]
    def getFiles(self, batchNum):
        return self.batchDict[batchNum]["files"]
    def concatFiles(self, filesToConcat, destination):
        concatCmd = shutil.which("cat") + " " + filesToConcat +  " >> " + destination
        startConcatTime = time.time()
        concatCmdStatus = subprocess.run(concatCmd, shell=True, capture_output=True, encoding="utf-8")
        concatTime = time.time() - startConcatTime
        if concatCmdStatus.returncode != 0:
            print("\tError concatenating files: " + concatCmdStatus.stderr)
            exit(1)
        print("\tFiles concatenated: " + filesToConcat + " to " + destination)
        print("\tTime: " + str(concatTime))


def counting():

    lazFilesforCount = glob.glob(storageInputDir + "/*.[lL][aA][zZ]")
    countDone = []
    numFilesdone = 0
    numBatches = 0
    #spaceleft = scratchSpaceAvailable
    batchCopier = BatchCopier(spaceUtilizationFactorCounting)
    while lazFilesforCount: #loop until all files are copied
        if batchCopier.gettotalSize() < scratchSpaceAvailable:
            size = 0
            filesToCopy = ""
            currlazFilesforCount = lazFilesforCount.copy()
            for laz in currlazFilesforCount:
                p = Path(laz)
                size += p.stat().st_size
                if size <= sizePerIterforCounting and  batchCopier.gettotalSize() + (spaceUtilizationFactorCounting * size) <= scratchSpaceAvailable:
                    filesToCopy += " " + laz
                    lazFilesforCount.remove(laz)
                else:
                    break
            batchCopier.copyFiles(filesToCopy, size, scratchInputDir)
            if batchCopier.gettotalSize() > scratchSpaceAvailable:
                print(bcolors.WARNING, "\tWARNING: Space utilization exceeded in counting after copying batch " + str(batchCopier.getNumBatchesCopied() - 1)  + ". Total space utilization:" + str(batchCopier.gettotalSize()) + " bytes, Max allowed:" + str(scratchSpaceAvailable) + " bytes", bcolors.ENDC)
            #spaceleft -= size
            #spaceleft = max(0, spaceleft)

            #batchCopier.setTotalSize(max(batchCopier.gettotalSize(),0))
            with open(scratchOutDir + "/.counting_copy_done_signals/batchno_" + str(batchCopier.getNumBatchesCopied() - 1) +"_copied", "w") as signalToPotreeConverterMPI: #signal to potree converter
                signalToPotreeConverterMPI.write(" ".join(map(lambda x: str(scratchInputDir + "/" + Path(x).name), filesToCopy.split()))) #write the copied files to the signal file
                if (lazFilesforCount == []):
                    signalToPotreeConverterMPI.write("\nlastbatch")
                else:
                    signalToPotreeConverterMPI.write("\nnotlastbatch")

        for signal in glob.glob(scratchOutDir + "/.counting_done_signals/batchno*"):
            PotreeConverterMPIsignalFile = Path(signal)
            batchNum =int(PotreeConverterMPIsignalFile.stem.split("_")[1])
            tend = time.time() - batchCopier.batchDict[batchNum]["starttime"]
            with open(signal, "r") as signalFile:
                duration = float(signalFile.read())
            batchCopier.addExecutionTime(duration)
            batchCopier.addWaitingTime(tend - duration)

            print("\tBatch-" +str(batchNum) + "/Partition-" + str(batchCopier.getPartitionNum(batchNum)) + " done counting in " +str(duration) + " seconds")
            countDone.append(str(batchNum) + "/" + str(batchCopier.getPartitionNum(batchNum)))
            numFilesdone += len(batchCopier.getFiles(batchNum).split())
            numBatches += 1
            #spaceleft += size
            batchCopier.removeBatch(batchNum)
            batchCopier.setTotalSize(max(0, batchCopier.gettotalSize()))
            PotreeConverterMPIsignalFile.unlink() #remove the signal file


        if not batchCopier.isbatchDictEmpty():
            print("\tCounting pending for batches/partitions: " + ",".join(map(lambda x: str(x) + "/" + str(batchCopier.getPartitionNum(x)), batchCopier.getCopiedBatchesKeys())) + ". Done batch/partition no.s: " + ",".join(countDone))
            time.sleep(60)
    while not batchCopier.isbatchDictEmpty():
        for signal in glob.glob(scratchOutDir + "/.counting_done_signals/batchno*"):
            PotreeConverterMPIsignalFile = Path(signal)
            batchNum =int(PotreeConverterMPIsignalFile.stem.split("_")[1])
            tend = time.time() - batchCopier.batchDict[batchNum]["starttime"]
            with open(signal, "r") as signalFile:
                duration = float(signalFile.read())
            batchCopier.addExecutionTime(duration)
            batchCopier.addWaitingTime(tend - duration)
            print("\tBatch-" +str(batchNum) + "/Partition-" + str(batchCopier.getPartitionNum(batchNum)) + " done counting in " +str(duration) + " seconds")
            countDone.append(str(batchNum) + "/" + str(batchCopier.getPartitionNum(batchNum)))
            numFilesdone += len(batchCopier.getFiles(batchNum).split())
            numBatches += 1
            #spaceleft += size
            batchCopier.removeBatch(batchNum)
            batchCopier.setTotalSize(max(0, batchCopier.gettotalSize()))
            PotreeConverterMPIsignalFile.unlink()

        if not batchCopier.isbatchDictEmpty():
            print("\tCounting pending for batches/partitions: " + ",".join(map(lambda x: str(x) + "/" + str(batchCopier.getPartitionNum(x)), batchCopier.getCopiedBatchesKeys())) +". Done batch/partition no.s: " + ",".join(countDone))
            time.sleep(60)
    #open(scratchOutDir + "/.counting_copy_done_signals/nomorefilesleftforcounting", "w").close()
    print("\tCounting finished. Total " + str(numFilesdone) + " files counted in " + str(numBatches) + " batches.")

def indexing(batches):
    #spaceleft = scratchSpaceAvailable
    batchCopier = BatchCopier(spaceUtilizationFactorIndexing)
    skipPartitions = []
    indexingDone = []
    numFilesdone = 0
    numBatches = 0
    totalBatches = len(batches)
    while batches:  # loop until all files are copied
        if batchCopier.gettotalSize() < scratchSpaceAvailable:
            currbatches = batches.copy()
            for lazBatch in currbatches:
                if lazBatch["status"] == "skip":
                    skipPartitions.append(lazBatch)
                    batches.remove(lazBatch)
                    continue
                if (spaceUtilizationFactorIndexing * lazBatch["size"]) + (batchCopier.gettotalSize()) <= scratchSpaceAvailable:
                    batchCopier.copyFiles( lazBatch["files"], lazBatch["size"], scratchInputDir, lazBatch["id"])
                    if batchCopier.gettotalSize() > scratchSpaceAvailable:
                        print(bcolors.WARNING, "\tWARNING: Space utilization exceeded in indexing after copying partition " + str(lazBatch["id"]) + ". Total space utilization:" + str(batchCopier.gettotalSize()) + " bytes, Max allowed:" + str(scratchSpaceAvailable) + " bytes", bcolors.ENDC)
                    batches.remove(lazBatch)
                   # spaceleft -= size
                    with open(scratchOutDir + "/.indexing_copy_done_signals/batchno_" + str(batchCopier.getNumBatchesCopied() - 1) + "_copied",
                              "w") as signalToPotreeConverterMPI:  # signal to potree converter
                        signalToPotreeConverterMPI.write(" ".join(map(lambda x: str(scratchInputDir + "/" + Path(x).name), lazBatch["files"].split())))  # write the copied files to the signal file
                        if (batches == []):
                            signalToPotreeConverterMPI.write("\nlastbatch")
                        else:
                            signalToPotreeConverterMPI.write("\nnotlastbatch")


        for signal in glob.glob(scratchOutDir + "/.indexing_done_signals/batchno*"):
            PotreeConverterMPIsignalFile = Path(signal)
            batchNum = int(PotreeConverterMPIsignalFile.stem.split("_")[1])
            tend = time.time() - batchCopier.batchDict[batchNum]["starttime"]
            with open(signal, "r") as signalFile:
                duration = float(signalFile.read())
            batchCopier.addExecutionTime(duration)
            batchCopier.addWaitingTime(tend - duration)
            print("\tBatch-" +str(batchNum) + "/Partition-" + str(batchCopier.getPartitionNum(batchNum)) + " indexed in " +str(duration) + " seconds")
            #spaceleft += size
            # with open(signal, "r") as chunks:
            #     chunkFiles = chunks.read()
            #     totalChunkSize = 0
            #     for chunk in chunkFiles.split():
            #         chunkPath = Path(chunk)
            #         totalChunkSize += chunkPath.stat().st_size
            #     batchCopier.removeFiles(chunkFiles)
            #     spaceleft += totalChunkSize
            # signal file for counting = Path(signal) #signal file for counting
            # remove the signal file
            filestoCat = []
            totalOctreeSize = 0
            for octree in glob.glob(scratchOutDir + "/octree*.bin"):
                filestoCat.append(octree)
                totalOctreeSize += Path(octree).stat().st_size
            filestoCat.sort(key=lambda x: int(Path(x).stem.split("_")[1]), reverse=True)
            concatStart = time.time()
            batchCopier.concatFiles(" ".join(filestoCat), storageOutDir + "/octree.bin")
            indexingDone.append(str(batchNum) + "/" + str(batchCopier.getPartitionNum(batchNum)))
            numFilesdone += len(batchCopier.getFiles(batchNum).split())
            numBatches += 1
            #print("\tConcatenated octree files  of " + str(batchNum) + " to " +  storageOutDir + "/octree.bin"  +" in " + str(time.time() - concatStart) + " seconds")
            batchCopier.removeFiles(" ".join(filestoCat))
            batchCopier.removeBatch(batchNum)
            batchCopier.setTotalSize(max(0, batchCopier.gettotalSize()))
            PotreeConverterMPIsignalFile.unlink()
            #batchCopier.setTotalSize(batchCopier.gettotalSize() - totalOctreeSize)
            open(scratchOutDir + "/.indexing_copy_done_signals/batchno_" + str(batchNum) + "_concatenated", "w").close()
        if not batchCopier.isbatchDictEmpty():
            print("\tIndexing pending for batches/partitions: " + ",".join(map(lambda x: str(x) + "/" + str(batchCopier.getPartitionNum(x)), batchCopier.getCopiedBatchesKeys())) + ". Skipped partition no.s :" + ",".join(map(lambda x: str(x["id"]), skipPartitions)) + ". Done batch/partition no.s: " + ",".join(indexingDone))
            time.sleep(60)
    
    while not batchCopier.isbatchDictEmpty():
        for signal in glob.glob(scratchOutDir + "/.indexing_done_signals/batchno*"):
            PotreeConverterMPIsignalFile = Path(signal)
            batchNum = int(PotreeConverterMPIsignalFile.stem.split("_")[1])
            tend = time.time() - batchCopier.batchDict[batchNum]["starttime"]
            with open(signal, "r") as signalFile:
                duration = float(signalFile.read())
            batchCopier.addExecutionTime(duration)
            batchCopier.addWaitingTime(tend - duration)
            print("\tBatch-" +str(batchNum) + "/Partition-" + str(batchCopier.getPartitionNum(batchNum)) + " indexed in " +str(duration) + " seconds")
            #spaceleft += size
            # with open(signal, "r") as chunks:
            #     chunkFiles = chunks.read()
            #     totalChunkSize = 0
            #     for chunk in chunkFiles.split():
            #         chunkPath = Path(chunk)
            #         totalChunkSize += chunkPath.stat().st_size
            #     batchCopier.removeFiles(chunkFiles)
            #     spaceleft += totalChunkSize
            # signal file for counting = Path(signal) #signal file for counting
            filestoCat = []
            totalOctreeSize = 0
            for octree in glob.glob(scratchOutDir + "/octree*.bin"):
                filestoCat.append(octree)
                totalOctreeSize += Path(octree).stat().st_size
            filestoCat.sort(key=lambda x: int(Path(x).stem.split("_")[1]), reverse=True)
            batchCopier.concatFiles(" ".join(filestoCat), storageOutDir + "/octree.bin")
            indexingDone.append(str(batchNum) + "/" + str(batchCopier.getPartitionNum(batchNum)))
            numFilesdone += len(batchCopier.getFiles(batchNum).split())
            numBatches += 1
            #print("\tConcatenated octree files  of " + str(batchNum) + " to " +  storageOutDir + "/octree.bin"  +" in " + str(time.time() - concatStart) + " seconds")
            batchCopier.removeFiles(" ".join(filestoCat))
            batchCopier.removeBatch(batchNum)
            batchCopier.setTotalSize(max(0, batchCopier.gettotalSize()))
            PotreeConverterMPIsignalFile.unlink()
        #batchCopier.setTotalSize(batchCopier.gettotalSize() - totalOctreeSize)
            open(scratchOutDir + "/.indexing_copy_done_signals/batchno_" + str(batchNum) + "_concatenated", "w").close()
        if not batchCopier.isbatchDictEmpty():
            print("\tIndexing pending for batches/partitions: " + ",".join(map(lambda x: str(x) + "/" + str(batchCopier.getPartitionNum(x)), batchCopier.getCopiedBatchesKeys())) + ". Skipped partition no.s:" + ",".join(map(lambda x: str(x["id"]), skipPartitions)) + ". Done batch/partition no.s: " + ",".join(indexingDone))
            time.sleep(60)
    print("\tIndexing finished. Total " + str(numFilesdone) + " files indexed in " + str(numBatches) + " partitions. Total partitions: " + str(totalBatches) + ", skipped " + str(len(skipPartitions)))



def createPartitions():
    print("Creating partitions...")

    lazfileStats = {}
    with open(AHN3partitions, "r", newline='') as partitionFile:
        partition = csv.reader(partitionFile)
        row1 = next(partition)
        for row in partition:
            if row[-1] not in lazfileStats:
                lazfileStats[row[-1]] = {}
                for col in row[0:-2]:
                    lazfileStats[row[-1]][row1[row.index(col)]] = [col]
            else:
                for col in row[0:-2]:
                    lazfileStats[row[-1]][row1[row.index(col)]].append(col)

    lazPartitions = []

    for id in lazfileStats:
        partitionSize = sum((Path(storageInputDir + "/" + url.split("/")[-1]).stat().st_size) for url in lazfileStats[id]["ahn3_url"])

        filesinPartition = {}
        filesinPartition["size"] = partitionSize
        filesinPartition["files"] = ""
        filesinPartition["id"] = id
        for url in lazfileStats[id]["ahn3_url"]:
            filesinPartition["files"] += " " + (storageInputDir + "/" + url.split("/")[-1])
            # lazPartitions.append(" ".join(filesinPartition))

        if max(spaceUtilizationFactorIndexing,spaceUtilizationFactorCounting) * partitionSize <= scratchSpaceAvailable:
            filesinPartition["status"] = "do"
        else:
            filesinPartition["status"] = "skip"
            print("Partition " + id + " size: " + str(partitionSize) + " bytes")
            print("Scratch space available: " + str(scratchSpaceAvailable) + " bytes")
            print("Scratch space needed: " + str( max(spaceUtilizationFactorIndexing,spaceUtilizationFactorCounting) * partitionSize) + " bytes")
            print("Partition too big for scratch space, skipping partition " + id)
        lazPartitions.append(filesinPartition)
    print("Done creating partitions. Total partitions: " + str(len(lazPartitions)))

    return lazPartitions

def PotreeConverterMPIBatched():

    print("Copier for counting started...")

    tStartCounting = time.time()
    counting()
    print("Copier for Counting finished in " + str(time.time() - tStartCounting) + " seconds")

    # exit(0)
    print("Copier for Indexing started...")
    tStartIndexing = time.time()
    lazpartitions = createPartitions()
    indexing(lazpartitions)
    print("Copier for Indexing finished in " + str(time.time() - tStartIndexing) + " seconds")
    print("Removing directories...")
    rmCmd = shutil.which("rm") + " -r " + " " + scratchOutDir + "/.counting_copy_done_signals" + " " + scratchOutDir + "/.indexing_copy_done_signals" + " " + scratchOutDir + "/.counting_done_signals" + " " + scratchOutDir + "/.indexing_done_signals"
    cmdOut=subprocess.run(rmCmd, shell=True, capture_output=True, encoding="utf-8")
    if cmdOut.returncode != 0:
        print("Error removing directories:\n" + cmdOut.stderr)
        exit(1)
    print("PotreeConverterMPI Job finished in " + str(time.time() - tStartCounting) + " seconds")
    exit(0)

def createDirectories():
    print("Creating directories...")

    if not Path(storageInputDir).exists():
        print("Storage input directory does not exist")
        exit(1)
    elif not Path(headersDir).exists():
        print("Headers directory for input data does not exist. It should be named " + scratchInputDir + "_headers")
        exit(1)
    else:
        pathsCount = 0
        for path in glob.glob(storageInputDir + "/*.[lL][aA][zZ]"):
            pathsCount += 1
            if not Path(headersDir + "/" + Path(path).stem + ".json").exists():
                print("Header file for " + Path(path).name + " does not exist. It should have the same name as the LAZ file with \".json\" extension and be in the " + headersDir + " directory")
                exit(1)
        if pathsCount == 0:
            print("No LAZ files found in storage input directory")
            exit(1)

    if Path(scratchInputDir).exists():
        print("Scratch input directory already exists. Do you want to overwrite it? (y/n)")
        answer = input()
        if answer == "y" or answer == "Y":
            shutil.rmtree(scratchInputDir)
        else:
            print("Exiting...")
            exit(0)

    Path(scratchInputDir).mkdir()

    if Path(scratchOutDir).exists():
        print("Scratch output directory already exists. Do you want to overwrite it? (y/n)")
        answer = input()
        if answer == "y" or answer == "Y":
            shutil.rmtree(scratchOutDir)
        else:
            print("Exiting...")
            exit(0)

    Path(scratchOutDir).mkdir()

    if Path(storageOutDir).exists():
        print("Storage output directory already exists. Do you want to overwrite it? (y/n)")
        answer = input()
        if answer == "y" or answer == "Y":
            shutil.rmtree(storageOutDir)
        else:
            print("Exiting...")
            exit(0)

    Path(storageOutDir).mkdir()



    # rmCmd= shutil.which("rm") + " -rf " + " " + scratchOutDir + "/.counting_copy_done_signals" + " " + scratchOutDir + "/.indexing_copy_done_signals" + " " + scratchOutDir + "/.counting_done_signals" + " " + scratchOutDir + "/.indexing_done_signals"
    # cmdOut = subprocess.run(rmCmd, shell=True, capture_output=True, encoding="utf-8")
    # if cmdOut.returncode != 0:
    #     print("Error removing directories:" + cmdOut.stderr)
    #     exit(1)
    mkCmd = shutil.which("mkdir") + " " + scratchOutDir + "/.counting_copy_done_signals" + " " + scratchOutDir + "/.indexing_copy_done_signals" + " " + scratchOutDir + "/.counting_done_signals" + " " + scratchOutDir + "/.indexing_done_signals"
    cmdOut=subprocess.run(mkCmd, shell=True, capture_output=True, encoding="utf-8")
    if cmdOut.returncode != 0:
        print("Error creating directories:\n" + cmdOut.stderr)
        exit(1)

if __name__ == "__main__":

    #
    # Create directories
    createDirectories()
    PotreeConverterMPICopier = Process(target=PotreeConverterMPIBatched)
    PotreeConverterMPICopier.start()

    PotreeConverterMPICopier.join()

    if PotreeConverterMPICopier.exitcode != 0:
        print("PotreeConverterMPIBatched exited with code " + str(PotreeConverterMPICopier.exitcode))
        exit(1)







