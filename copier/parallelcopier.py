import itertools
import multiprocessing
import shutil
import subprocess
import time
from pathlib import Path
from copier import Copier

def runCopyCmd(files, destination):
    copyCmd = shutil.which("cp") + " " + " ".join(files) + " " + destination
    try:
        subprocess.run(copyCmd, shell=True, capture_output=True, encoding="utf-8", check=True)
    except subprocess.CalledProcessError as e:
        print("\033[1;31mError copying files:\033[0m " + e.stderr)
        exit(1)
class ParallelCopier(Copier):
    def fileChunks(self, filesList, nChunks):
        chunkSize = len(filesList) // nChunks
        remainder = len(filesList) % nChunks
        currentIndex = 0
        for i in range(nChunks):
            additional = 1 if i < remainder else 0
            yield filesList[currentIndex:currentIndex + chunkSize + additional]
            currentIndex += chunkSize + additional


    def copyFiles(self, filesToCopy, destination):
        subsets = self.filesSubsets(filesToCopy)
        for subset in subsets:
            numProcesses = multiprocessing.cpu_count() if len(subset) >= multiprocessing.cpu_count() else len(subset)
            chunks = list(self.fileChunks(subset, numProcesses))
            with multiprocessing.Pool(numProcesses) as pool:
                pool.starmap(runCopyCmd, list(zip(chunks, itertools.repeat(destination))))



