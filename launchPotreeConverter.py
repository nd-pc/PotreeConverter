#!/usr/bin/python3

import glob
import subprocess
from pathlib import Path
import time


sizecopiedPerIter = 5E9

SCRATH_INPUT_DIR = "ls /scratch/anauman/escience/projects/nD-PC/ahn3"

lazFiles = glob.glob("/home/anauman/staff-umbrella/ahn3/*.[lL][aA][zZ]")

lazFilesforCount = lazFiles.copy()


copiedFiles = ""
numCopied = 0
batchCopied = 0
filesToremove = {"", ""}
averageCountingThroughput=0 #
averageCopyingThroughput=0

while True: #loop until all files are copied
    if filesToremove[0] != "": #remove the files from the previous iteration
        rmCmdStatus = subprocess.run("rm " + filesToremove[0])
        if rmCmdStatus.returncode != 0:
            print("Error removing file " + filesToremove[0])
            exit(1)
        filesToremove[0] = ""
    if len(lazFilesforCount) == 0: #if all files are copied,
        break
    totolSize = 0
    for laz in lazFilesforCount: #
        p = Path(laz)
        totolSize += p.stat().st_size
        if totolSize <= sizecopiedPerIter:
            copiedFiles += " " + laz
            lazFilesforCount.remove(laz)
            numCopied += 1
        else:
            break

    copyCmd = "cp " + copiedFiles + " /scratch/anauman/escience/projects/nD-PC/ahn3"
    print(copyCmd)
    print("Copying ...")
    startCopyTime = time.time()
    copyCmdStatus = subprocess.run(copyCmd)
    copyTime = time.time() - startCopyTime
    if copyCmdStatus.returncode != 0:
        print("Error copying files: " + copiedFiles)
        exit(1)
    batchCopied += 1
    averageCopyingThroughput += (totolSize / copyTime)/batchCopied

    print("Copied batch number" + str(batchCopied) + "of " + str(numCopied) + " files in" + str(
        copyTime) + " seconds. Average Throughput: " + str(averageCopyingThroughput) + " MB/s")
    print("Files copied: " + str(copiedFiles))
    signalToPotreeConverter = open(".batchno" + str(batchCopied) +"copied.txt", "w") #signal to potree converter
    signalToPotreeConverter.write(copiedFiles) #write the copied files to the signal file
    signalToPotreeConverter.close()
    PotreeConvertersignalFile = Path(".countingstartedforbatchno" + str(batchCopied) + ".txt") #signal file for counting
    while not PotreeConvertersignalFile.exists(): #wait for the counting to start
        time.sleep(10)
    countingTime = float(open(".countingstartedforbatchno" + str(batchCopied) + ".txt", "r").readline()) #read the counting time
    if countingTime != 0:
        averageCountingThroughput += (totolSize / countingTime)/batchCopied
    try:
        PotreeConvertersignalFile.unlink()
    except OSError as e:
        print("Error removing file " + PotreeConvertersignalFile)
        exit(1)

    filesToremove[0] = copiedFiles


    if filesToremove[1] != "": #remove the files from the previous iteration
        rmCmdStatus = subprocess.run("rm " + filesToremove[1])
        if rmCmdStatus.returncode != 0:
            print("Error removing file " + filesToremove[1])
            exit(1)
        filesToremove[1] = ""
    if len(lazFilesforCount) == 0:
        break
    totolSize = 0
    for laz in lazFilesforCount:
        p = Path(laz)
        totolSize += p.stat().st_size
        if totolSize <= sizecopiedPerIter:
            copiedFiles += " " + laz
            lazFilesforCount.remove(laz)
            numCopied += 1
        else:
            break

    copyCmd = "cp " + copiedFiles + " /scratch/anauman/escience/projects/nD-PC/ahn3"
    print(copyCmd)
    print("Copying ...")
    startCopyTime = time.time()
    copyCmdStatus = subprocess.run(copyCmd)
    copyTime = time.time() - startCopyTime
    if copyCmdStatus.returncode != 0:
        print("Error copying files:" + copiedFiles)
        exit(1)
    batchCopied += 1
    averageCopyingThroughput += (totolSize / copyTime)/batchCopied

    print("Copied batch number" + str(batchCopied) + "of " + str(numCopied) + " files in" + str(
        copyTime) + " seconds. Average Throughput: " + str(averageCopyingThroughput) + " MB/s")
    print("Files copied: " + str(copiedFiles))

    signalToPotreeConverter = open(".batchno" + str(batchCopied) +"copied.txt", "w")
    signalToPotreeConverter.write(copiedFiles)
    signalToPotreeConverter.close()
    PotreeConvertersignalFile = Path(".countingstartedforbatchno" + str(batchCopied) + ".txt")
    while not PotreeConvertersignalFile.exists():
        time.sleep(60)
    try:
        PotreeConvertersignalFile.unlink()
    except OSError as e:
        print("Error removing file " + PotreeConvertersignalFile)
        exit(1)
    filesToremove[1] = copiedFiles

#--------------------------------------------------------------------------------

signalToPotreeConverter = open(".nomorefilesleftforcounting.txt", "w")
signalToPotreeConverter.close()
PotreeConvertersignalFile = Path(".countingfinshed.txt")
while not PotreeConvertersignalFile.exists():
    time.sleep(10)

countingTime = float(open(".countingstartedforbatchno" + str(batchCopied) + ".txt", "r").readline())
if countingTime != 0:
    averageCountingThroughput += (totolSize / countingTime)/batchCopied
try:
    PotreeConvertersignalFile.unlink()
except OSError as e:
    print("Error removing file " + PotreeConvertersignalFile)
    exit(1)
rmCmdStatus = subprocess.run("rm " + "/scratch/anauman/escience/projects/nD-PC/ahn3/*.[lL][aA][zZ]")
if rmCmdStatus.returncode != 0:
    print("Error removing file " + " ".join(glob.gob("/scratch/anauman/escience/projects/nD-PC/ahn3/*.[lL][aA][zZ]")))
    exit(1)


#print(lazFiles)

