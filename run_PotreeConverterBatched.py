
import sys
import time
import psutil


from multiprocessing import Process

from potreeconverterpartitioned import PotreeConverterBatched

from potreeconverterpartitioned.loggingwrapper import LoggingWrapper

from potreeconverterpartitioned.copier import SimpleCopier


def PotreeConverterMPIBatchedCopier(potreeConverterBatched):
    potreeConverterBatched.counting()
    potreeConverterBatched.indexing()

    exit(0)


if __name__ == "__main__":
    '''The run script fpr PotreeConverterMPI. It creates the directories, launches the job and monitors it.'''

    if len(sys.argv) != 2:
        LoggingWrapper.error("Usage: python3 run_PotreeConverterBatched.py <path to config.ini file>")
        exit(1)
    configFilePath = sys.argv[1]
    potreeConverterBatched = PotreeConverterBatched(configFilePath)
    LoggingWrapper.info("Submitting PotreeConverterMPI job", color="blue", bold=True)
    potreeConverterBatched.scheduler.launchJob()

    # Start the partition loader
    PotreeConverterMPICopier = Process(target=PotreeConverterMPIBatchedCopier, args=(potreeConverterBatched,))
    PotreeConverterMPICopier.start()

    #while PotreeConverterMPI job and partition loader are running, wait
    while potreeConverterBatched.scheduler.isJobAlive() and PotreeConverterMPICopier.is_alive():
        time.sleep(10)

    # If the partion loader failed, kill the PotreeConverterMPI job
    if potreeConverterBatched.scheduler.isJobAlive() and PotreeConverterMPICopier.exitcode != 0 :
        LoggingWrapper.error("Batch copier failed")
        potreeConverterBatched.scheduler.killJob()
        exit(1)
    # If the PotreeConverterMPI job failed, kill the partition loader
    elif potreeConverterBatched.scheduler.getJobExitCode() != 0 and PotreeConverterMPICopier.is_alive():
        parent = psutil.Process(PotreeConverterMPICopier.pid)
        for child in parent.children(recursive=True):
            child.kill()
        PotreeConverterMPICopier.terminate()
        LoggingWrapper.error("Batch copier terminated on PotreeConverterMPI failure/cancel")
        exit(1)
    # If both jobs finished successfully, exit
    elif potreeConverterBatched.scheduler.getJobExitCode() != 0 and PotreeConverterMPICopier.exitcode != 0:
        LoggingWrapper.error("Batch copier and PotreeConverterMPI  failed")
        exit(1)
    # If partition loader finished successfully, wait for PotreeConverterMPI to finish
    elif potreeConverterBatched.scheduler.isJobAlive()  and PotreeConverterMPICopier.exitcode == 0:
        LoggingWrapper.info("Batch copier finished successfully. Waiting for PotreeConverterMPI to finish...", color="green")
        while potreeConverterBatched.scheduler.isJobAlive():
            time.sleep(10)
        if potreeConverterBatched.scheduler.getJobExitCode() != 0:
            LoggingWrapper.error("PotreeConverterMPI failed. However, batch copier finished successfully")
            exit(1)

    # If PotreeConverterMPI finished successfully, wait for partition loader to finish
    elif potreeConverterBatched.scheduler.getJobExitCode() == 0 and PotreeConverterMPICopier.is_alive():
        LoggingWrapper.info("PotreeConverterMPI finished successfully. Waiting for batch copier to finish...", color="green")
        while PotreeConverterMPICopier.is_alive():
            time.sleep(10)
        if PotreeConverterMPICopier.exitcode != 0:
            LoggingWrapper.error("Batch copier failed. However, PotreeConverterMPI finished successfully")
            exit(1)

    copyHierMeta = SimpleCopier()
    copyHierMeta.copyFiles([potreeConverterBatched.tmpOutputDir + "/hierarchy.bin"], potreeConverterBatched.OutputDir)
    copyHierMeta.copyFiles([potreeConverterBatched.tmpOutputDir + "/metadata.json"], potreeConverterBatched.OutputDir)
    LoggingWrapper.info("Batch copier and PotreeConverterMPI finished successfully", color="green", bold=True)

