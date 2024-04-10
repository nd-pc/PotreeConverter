import shutil
import subprocess
from .copier import Copier

from potreeconverterpartitioned.loggingwrapper import LoggingWrapper



class SimpleCopier(Copier):


    def copyFiles(self, filesToCopy, destination):
        """Copies the files to the destination directory
        :param filesToCopy: the list of files to copy"""
        subsets = self.filesSubsets(filesToCopy)
        for subset in subsets:
            copyCmd = shutil.which("cp") + " " + " ".join(subset) + " " + destination
            copyCmdStatus = subprocess.run(copyCmd, shell=True, capture_output=True, encoding="utf-8")
            if copyCmdStatus.returncode != 0:
                LoggingWrapper.error("Error copying files: " + copyCmdStatus.stderr)
                exit(1)



