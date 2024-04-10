import itertools
import multiprocessing
import shutil
import subprocess
from .copier import Copier

from potreeconverterpartitioned.loggingwrapper import LoggingWrapper


class ParallelCopier(Copier):
    '''Copies files in parallel by splitting the files  and copying in parallel'''
    def splitFiles(self, filesList, nSplits):
        '''Splits the files into nSplits
        :param filesList: the list of files to split
        :param nChunks: the number of chunks to split the files into'''
        splitSize = len(filesList) // nSplits
        remainder = len(filesList) % nSplits
        currentIndex = 0
        for i in range(nSplits):
            additional = 1 if i < remainder else 0
            yield filesList[currentIndex:currentIndex + splitSize + additional]
            currentIndex += splitSize + additional


    def copyFiles(self, filesToCopy, destination):
        '''Copies the files to the destination directory
        :param filesToCopy: the list of files to copy
        :param destination: the destination directory to copy the files to'''
        subsets = self.filesSubsets(filesToCopy)
        for subset in subsets:
            numProcesses = multiprocessing.cpu_count() if len(subset) >= multiprocessing.cpu_count() else len(subset)
            numSplits = list(self.splitFiles(subset, numProcesses))
            with multiprocessing.Pool(numProcesses) as pool:
                pool.starmap(self.runCopyCmd, list(zip(numSplits, itertools.repeat(destination))))

    def runCopyCmd(self, files, destination):
        '''Runs the copy command
        :param files: the list of files to copy
        :param destination: the destination directory to copy the files to'''
        copyCmd = shutil.which("cp") + " " + " ".join(files) + " " + destination

        copyCmdStatus = subprocess.run(copyCmd, shell=True, capture_output=True, encoding="utf-8")
        if copyCmdStatus.returncode != 0:
            LoggingWrapper.error("Error copying files: " + copyCmdStatus.stderr)
            exit(1)
