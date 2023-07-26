

import glob
import subprocess
from datetime import datetime
from pathlib import Path
import time
import csv
import psutil
import shutil


from multiprocessing import Process, Queue

scratchSpaceAvailable = 17*(1024)**3 #

sizePerIterforCounting = 1*(1024)**3 #1 GB

spaceUtilizationFactorCounting = 1
spaceUtilizationFactorIndexing = 5

storageInputDir = "/home/anauman/staff-umbrella/ahn3_sim_data"

scratchInputDir = "/scratch/anauman/escience/projects/nD-PC/ahn3_sim_data"

scratchOutDir = scratchInputDir + "_PotreeConverterMPI_output"

storageOutDir = storageInputDir + "_PotreeConverterMPI_output"

AHN3partitions = "ahn3_partitions_4x4.csv"

sbatchScript = "/home/anauman/escience/projects/nD-PC/PotreeConverter/delftblue_cluster.sh"

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
            self.removeFiles(self.batchDict[batchNum]["copiedfiles"])
        else:
            print("Error: batch number " + str(batchNum) + " does not exist")
            exit(1)
        size = self.batchDict[batchNum]["size"]
        self.totalSize -= self.storageUtilizationFactor * size #update the total size
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


    def copyFiles(self, filesToCopy, size,  destination):
        subsets = self.filesSubsets(filesToCopy)
        startCopyTime = time.time()
        for subset in subsets:
            copyCmd = shutil.which("cp") + " " + subset + " " + destination
            copyCmdStatus = subprocess.run(copyCmd, shell=True, capture_output=True, encoding="utf-8")
            if copyCmdStatus.returncode != 0:
                print("Error copying files: " + copyCmdStatus.stderr)
                exit(1)
        copyTime = time.time() - startCopyTime
        self.batchCopied += 1
        self.batchDict[self.batchCopied] = {"copiedfiles": " ".join(map(lambda x: str(destination + "/" + Path(x).name), filesToCopy.split())), "size": size, "starttime":time.time()}
        self.totalSize += self.storageUtilizationFactor * size
        self.totalCopyRemoveTime += copyTime
        print("\tBatch " + str(self.batchCopied) +  " copied")#: " + ",".join(map(lambda x: Path(x).name, filesToCopy.split())))
        print("\tSize: " + str(size) + " bytes")
        print("\tTime: " + str(copyTime))
        print("\tCopying throughput: " + str((size / copyTime) / (1024**2)) + " MB/s")
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
    def concatFiles(self, filesToConcat, destination):
        concatCmd = shutil.which("cat") + " " + filesToConcat +  " >> " + destination
        startConcatTime = time.time()
        concatCmdStatus = subprocess.run(concatCmd, shell=True, capture_output=True, encoding="utf-8")
        concatTime = time.time() - startConcatTime
        if concatCmdStatus.returncode != 0:
            print("\tError concatenating files: " + concatCmdStatus.stderr)
            exit(1)
        print("\tFiles concatenated: " + filesToConcat)
        print("\tTime: " + str(concatTime))


def counting():

    lazFilesforCount = glob.glob(storageInputDir + "/*.[lL][aA][zZ]")
    #spaceleft = scratchSpaceAvailable
    batchCopier = BatchCopier(spaceUtilizationFactorCounting)
    while lazFilesforCount: #loop until all files are copied
        if batchCopier.gettotalSize() < scratchSpaceAvailable:
            size = 0
            filesToCopy = ""
            for laz in lazFilesforCount:
                p = Path(laz)
                size += p.stat().st_size
                if size <= sizePerIterforCounting:
                    filesToCopy += " " + laz
                    lazFilesforCount.remove(laz)
                else:
                    break
            batchCopier.copyFiles(filesToCopy, size, scratchInputDir)
            #spaceleft -= size
            #spaceleft = max(0, spaceleft)
            batchCopier.setTotalSize(max(batchCopier.gettotalSize(),0))
            with open(scratchOutDir + "/.counting_copy_done_signals/batchno_" + str(batchCopier.getNumBatchesCopied()) +"_copied", "w") as signalToPotreeConverterMPI: #signal to potree converter
                signalToPotreeConverterMPI.write(" ".join(map(lambda x: str(scratchInputDir + "/" + Path(x).name), filesToCopy.split()))) #write the copied files to the signal file
                if (lazFilesforCount == []):
                    signalToPotreeConverterMPI.write("\nlastbatch")
                else:
                    signalToPotreeConverterMPI.write("\nnotlastbatch")
        else:
            if not batchCopier.isbatchDictEmpty():
                print("\tWaiting for counting to finish for batches " + ",".join(map(str, batchCopier.getCopiedBatchesKeys())))
            time.sleep(60)
        for signal in glob.glob(scratchOutDir + "/.counting_done_signals/batchno*"):
            PotreeConverterMPIsignalFile = Path(signal)
            batchNum =int(PotreeConverterMPIsignalFile.stem.split("_")[1])
            tend = time.time() - batchCopier.batchDict[batchNum]["starttime"]
            with open(signal, "r") as signalFile:
                duration = float(signalFile.read())
            batchCopier.addExecutionTime(duration)
            batchCopier.addWaitingTime(tend - duration)

            print("\tBatch " +str(batchNum) + " done counting in " +str(duration) + " seconds")
            #spaceleft += size
            batchCopier.removeBatch(batchNum)
            PotreeConverterMPIsignalFile.unlink() #remove the signal file
            batchCopier.setTotalSize(min(scratchSpaceAvailable, batchCopier.gettotalSize()))

    while not batchCopier.isbatchDictEmpty():
        for signal in glob.glob(scratchOutDir + "/.counting_done_signals/batchno*"):
            PotreeConverterMPIsignalFile = Path(signal)
            batchNum =int(PotreeConverterMPIsignalFile.stem.split("_")[1])
            tend = time.time() - batchCopier.batchDict[batchNum]["starttime"]
            with open(signal, "r") as signalFile:
                duration = float(signalFile.read())
            batchCopier.addExecutionTime(duration)
            batchCopier.addWaitingTime(tend - duration)
            print("\tBatch " + str(batchNum) + " done counting in " + str(duration) + " seconds")
            #spaceleft += size
            batchCopier.removeBatch(batchNum)
            PotreeConverterMPIsignalFile.unlink()
        batchCopier.setTotalSize(min(scratchSpaceAvailable, batchCopier.gettotalSize()))
        if not batchCopier.isbatchDictEmpty():
            print("\tWaiting for counting to finish for batches " + ",".join(map(str, batchCopier.getCopiedBatchesKeys())))
        time.sleep(60)
    #open(scratchOutDir + "/.counting_copy_done_signals/nomorefilesleftforcounting", "w").close()


def indexing(batches):
    #spaceleft = scratchSpaceAvailable
    batchCopier = BatchCopier(spaceUtilizationFactorIndexing)
    while batches:  # loop until all files are copied
        if batchCopier.gettotalSize() < scratchSpaceAvailable:
            filesToCopy = ""
            for lazBatch in batches:
                size = lazBatch["size"]
                if (spaceUtilizationFactorIndexing * size) + (batchCopier.gettotalSize()) <= scratchSpaceAvailable:
                    filesToCopy += " " + lazBatch["files"]
                    batchCopier.copyFiles(filesToCopy, size, scratchInputDir)
                    batches.remove(lazBatch)
                   # spaceleft -= size
                    with open(scratchOutDir + "/.indexing_copy_done_signals/batchno_" + str(batchCopier.getNumBatchesCopied()) + "_copied",
                              "w") as signalToPotreeConverterMPI:  # signal to potree converter
                        signalToPotreeConverterMPI.write(" ".join(map(lambda x: str(scratchInputDir + "/" + Path(x).name), filesToCopy.split())))  # write the copied files to the signal file
                        if (batches == []):
                            signalToPotreeConverterMPI.write("\nlastbatch")
                        else:
                            signalToPotreeConverterMPI.write("\nnotlastbatch")

        else:
            if not batchCopier.isbatchDictEmpty():
                print("\tWaiting for indexing to finish for batches " + ",".join(map(str, batchCopier.getCopiedBatchesKeys())))
            time.sleep(60)
        for signal in glob.glob(scratchOutDir + "/.indexing_done_signals/batchno*"):
            PotreeConverterMPIsignalFile = Path(signal)
            batchNum = int(PotreeConverterMPIsignalFile.stem.split("_")[1])
            tend = time.time() - batchCopier.batchDict[batchNum]["starttime"]
            with open(signal, "r") as signalFile:
                duration = float(signalFile.read())
            batchCopier.addExecutionTime(duration)
            batchCopier.addWaitingTime(tend - duration)
            print("\tBatch " +str(batchNum) + " indexed in " +str(duration) + "seconds")
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
            batchCopier.removeBatch(batchNum)
            PotreeConverterMPIsignalFile.unlink()  # remove the signal file

            filestoCat = []
            totalOctreeSize = 0
            for octree in glob.glob(scratchOutDir + "/octree*.bin"):
                octreePath = Path(octree)
                filestoCat += octree
                totalOctreeSize += octreePath.stat().st_size
                filestoCat.sort(key=lambda x: int(Path(x).stem.split("_")[1]), reverse=True)

            concatStart = time.time()
            batchCopier.concatFiles(" ".join(filestoCat), storageOutDir + "/octree.bin")
            batchCopier.removeFiles(" ".join(filestoCat))
            print("\tConcatenated octree files  of " + str(batchNum) + " in " + str(time.time() - concatStart) + " seconds")
            #batchCopier.setTotalSize(batchCopier.gettotalSize() - totalOctreeSize)
            open(scratchOutDir + "/.indexing_copy_done_signals/batchno_" + str(batchNum) + "_concatenated", "w").close()
            batchCopier.setTotalSize(min(scratchSpaceAvailable, batchCopier.gettotalSize()))
    #open(scratchOutDir + "/.indexing_copy_done_signals/nomorefilesleftforindexing", "w").close()
    while not batchCopier.isbatchDictEmpty():
        for signal in glob.glob(scratchOutDir + "/.indexing_done_signals/batchno*"):
            PotreeConverterMPIsignalFile = Path(signal)
            batchNum = int(PotreeConverterMPIsignalFile.stem.split("_")[1])
            tend = time.time() - batchCopier.batchDict[batchNum]["starttime"]
            with open(signal, "r") as signalFile:
                duration = float(signalFile.read())
            batchCopier.addExecutionTime(duration)
            batchCopier.addWaitingTime(tend - duration)
            print("\tBatch " +str(batchNum) + " done counting in " +str(duration) + "seconds")
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
            batchCopier.removeBatch(batchNum)
            PotreeConverterMPIsignalFile.unlink()
            filestoCat = []
            totalOctreeSize = 0
            for octree in glob.glob(scratchOutDir + "/octree*.bin"):
                octreePath = Path(octree)
                filestoCat += octree
                totalOctreeSize += octreePath.stat().st_size
                filestoCat.sort(key=lambda x: int(Path(x).stem.split("_")[1]), reverse=True)
            concatStart = time.time()
            batchCopier.concatFiles(" ".join(filestoCat), storageOutDir + "/octree.bin")
            batchCopier.removeFiles(" ".join(filestoCat))
            print("\tConcatenated octree files  of " + str(batchNum) + " in " + str(time.time() - concatStart) + " seconds")
            #batchCopier.setTotalSize(batchCopier.gettotalSize() - totalOctreeSize)

            open(scratchOutDir + "/.indexing_copy_done_signals/batchno_" + str(batchNum) + "_concatenated", "w").close()
            batchCopier.setTotalSize(min(batchCopier.gettotalSize(), scratchSpaceAvailable))
        if not batchCopier.isbatchDictEmpty():
            print("\tWaiting for indexing to finish for batches " + ",".join(map(str, batchCopier.getCopiedBatchesKeys())))
        time.sleep(60)


def monitorPotreeConverterMPIJob(q):
    # Submit a job with sbatch and get the job id
    print("Submitting PotreeConverterMPI job...")
    cmd = shutil.which("sbatch")  + " " + sbatchScript + " " + scratchOutDir + " " + scratchInputDir + "_headers"
    process = subprocess.run(cmd, shell=True,  capture_output=True, encoding="utf-8")
    if process.returncode != 0:
        print("Something went wrong in submitting the job")
        exit(1)
    output = process.stdout
    JID = output.split()[-1]
    print("Submitted PotreeConverterMPI job with job id: " + JID)


    q.put(JID)



    ST = "PENDING"  # Status to be checked

    while not ST.startswith("RUNNING"):
        print("Waiting for PotreeConverterMPI job to start...")
        cmd = shutil.which("sacct") + " -j " + JID + " -o State"
        process = subprocess.run(cmd, shell=True,  capture_output=True, encoding="utf-8")
        if process.returncode != 0:
            print("Something went wrong in checking the job status")
            exit(1)
        output = process.stdout
        ST = output.split()[-1]

        time.sleep(15)

    print("PotreeConverterMPI job started at " + str(datetime.now()))
    # Monitoring loop
    while not ST.startswith("COMPLETED"):
        cmd = shutil.which("sacct") + " -j " + JID + " -o State"
        process = subprocess.run(cmd, shell=True,  capture_output=True, encoding="utf-8")
        if process.returncode != 0:
            print("Something went wrong in checking the job status")
            exit(1)
        output = process.stdout
        ST = output.split()[-1]

        time.sleep(15)  # Time interval between checks

        if ST.startswith("FAILED"):
            print(f"PotreeConverterMPI job failed")  # Show humans some info if the job fails
            exit(1)
        elif ST.startswith("CANCELLED"):
            print(f"PotreeConverterMPI job cancelled")  # Show humans some info if the job is cancelled
            exit(1)
        elif ST.startswith("TIMEOUT"):
            print(f"PotreeConverterMPI job timeout")
            exit(1)

    print(f"PotreeConverterMPI Job finished")  # Show humans some info when the job finishes
    exit(0)


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
        if max(spaceUtilizationFactorIndexing,spaceUtilizationFactorCounting) * partitionSize <= scratchSpaceAvailable:
            filesinPartition = {}
            filesinPartition["size"] = partitionSize
            filesinPartition["files"] = ""
            for url in lazfileStats[id]["ahn3_url"]:
                filesinPartition["files"] += " " + (storageInputDir + "/" + url.split("/")[-1])
                # lazPartitions.append(" ".join(filesinPartition))
            lazPartitions.append(filesinPartition)
        else:
            print("Partition " + id + " size: " + str(partitionSize) + " bytes")
            print("Scratch space available: " + str(scratchSpaceAvailable) + " bytes")
            print("Scratch space needed: " + str( max(spaceUtilizationFactorIndexing,spaceUtilizationFactorCounting) * partitionSize) + " bytes")
            print("Partition too big for scratch space, skipping partition " + id)

    print("Done creating partitions")

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
    elif not Path(scratchInputDir + "_headers").exists():
        print("Headers directory for input data does not exist. It should be named " + scratchInputDir + "_headers")
        exit(1)
    else:
        pathsCount = 0
        for path in glob.glob(storageInputDir + "/*.[lL][aA][zZ]"):
            pathsCount += 1
            if not Path(scratchInputDir + "_headers" + "/" + Path(path).stem + ".json").exists():
                print("Header file for " + Path(path).name + " does not exist. It should have the same name as the LAZ file with \".json\" extension and be in the " + scratchInputDir + "_headers directory")
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
    q = Queue()
    PotreeConverterMPIMonitorProcess = Process(target=monitorPotreeConverterMPIJob, args=(q,))
    PotreeConverterMPIMonitorProcess.start()
    PotreeConverterMPICopier = Process(target=PotreeConverterMPIBatched)
    PotreeConverterMPICopier.start()

    while PotreeConverterMPIMonitorProcess.is_alive() and PotreeConverterMPICopier.is_alive():
        time.sleep(30)

    if PotreeConverterMPICopier.exitcode != 0 and PotreeConverterMPIMonitorProcess.is_alive():
        print("PotreeConverterMPICopier failed")
        cancelCmd = shutil.which("scancel") + " " + str(q.get())
        cmdOut = subprocess.run(cancelCmd, shell=True, capture_output=True, encoding="utf-8")
        if cmdOut.returncode != 0:
            print("Error cancelling job:\n" + cmdOut.stderr)
        exit(1)
    elif PotreeConverterMPIMonitorProcess.exitcode != 0 and PotreeConverterMPICopier.is_alive():
        parent = psutil.Process(PotreeConverterMPICopier.pid)
        for child in parent.children(recursive=True):
            child.kill()
        PotreeConverterMPICopier.terminate()
        print("PotreeConverterMPICopier terminated")
        exit(1)
    elif PotreeConverterMPIMonitorProcess.exitcode != 0 and PotreeConverterMPICopier.exitcode != 0:
        print("PotreeConverterMPICopier failed")
        exit(1)
    else:
        print("PotreeConverterMPI and PotreeConverterMPICopier done")
        exit(0)






