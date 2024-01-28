import itertools
import multiprocessing
import shutil
import subprocess
from copier import Copier

from ..loggingwrapper import LoggingWrapper


class ParallelCopier(Copier):
    '''Copies files in parallel command'''
    def fileChunks(self, filesList, nChunks):
        '''Splits the files into chunks of size nChunks
        :param filesList: the list of files to split
        :param nChunks: the number of chunks to split the files into'''
        chunkSize = len(filesList) // nChunks
        remainder = len(filesList) % nChunks
        currentIndex = 0
        for i in range(nChunks):
            additional = 1 if i < remainder else 0
            yield filesList[currentIndex:currentIndex + chunkSize + additional]
            currentIndex += chunkSize + additional


    def copyFiles(self, filesToCopy, destination):
        '''Copies the files to the destination directory
        :param filesToCopy: the list of files to copy
        :param destination: the destination directory to copy the files to'''
        subsets = self.filesSubsets(filesToCopy)
        for subset in subsets:
            numProcesses = multiprocessing.cpu_count() if len(subset) >= multiprocessing.cpu_count() else len(subset)
            chunks = list(self.fileChunks(subset, numProcesses))
            with multiprocessing.Pool(numProcesses) as pool:
                pool.starmap(self.runCopyCmd, list(zip(chunks, itertools.repeat(destination))))

    def runCopyCmd(self, files, destination):
        '''Runs the copy command
        :param files: the list of files to copy
        :param destination: the destination directory to copy the files to'''
        copyCmd = shutil.which("cp") + " " + " ".join(files) + " " + destination

        copyCmdStatus = subprocess.run(copyCmd, shell=True, capture_output=True, encoding="utf-8")
        if copyCmdStatus.returncode != 0:
            LoggingWrapper.error("Error copying files: " + copyCmdStatus.stderr)
            exit(1)
