import shutil
import subprocess
import time
from pathlib import Path
from copier import Copier

class SimpleCopier(Copier):

    def copyFiles(self, filesToCopy, destination):
        subsets = self.filesSubsets(filesToCopy)
        for subset in subsets:
            copyCmd = shutil.which("cp") + " " + " ".join(subset) + " " + destination
            copyCmdStatus = subprocess.run(copyCmd, shell=True, capture_output=True, encoding="utf-8")
            if copyCmdStatus.returncode != 0:
                self.logger.error("Error copying files: " + copyCmdStatus.stderr)
                exit(1)



