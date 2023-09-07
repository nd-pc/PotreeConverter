from pathlib import Path
import csv

AHN3partitions = "ahn3_partitions_4x4.csv"
storageInputDir = "/home/anauman/staff-umbrella/ahn3_sim_data_2"

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
        lazPartitions.append(filesinPartition)
    print("Done creating partitions. Total partitions: " + str(len(lazPartitions)))
    return lazPartitions
def createPartitionsnew():
    print("Creating partitions...")

    lazfileStats = {}
    with open(AHN3partitions, "r", newline='') as partitionFile:
        partition = csv.reader(partitionFile)
        row1 = next(partition)
        bladnrIdx = row1.index("bladnr")
        partitionIdIdx = row1.index("partition_id")
        for row in partition:
            if row[partitionIdIdx] not in lazfileStats:
                lazfileStats[row[partitionIdIdx]] = {}
                lazfileStats[row[partitionIdIdx]]["bladnr"] = [row[bladnrIdx]]
            else:
                lazfileStats[row[partitionIdIdx]]["bladnr"].append(row[bladnrIdx])

    #lazPartitions = []

    for id in lazfileStats:
        #partitionSize = sum((Path(storageInputDir + "/C_" + bladnr.upper() + ".LAZ").stat().st_size) for bladnr in lazfileStats[id]["bladnr"])
        for bladnr in lazfileStats[id]["bladnr"]:
            Path(storageInputDir + "/C_" + bladnr.upper() + ".LAZ").unlink()
    #     filesinPartition = {}
    #     filesinPartition["size"] = partitionSize
    #     filesinPartition["files"] = ""
    #     filesinPartition["id"] = id
    #     for bladnr in lazfileStats[id]["bladnr"]:
    #         filesinPartition["files"] += " " + (storageInputDir + "/C_" + bladnr.upper() + ".LAZ")
    #         # lazPartitions.append(" ".join(filesinPartition))
    #     lazPartitions.append(filesinPartition)
    # print("Done creating partitions. Total partitions: " + str(len(lazPartitions)))
    # return lazPartitions
if __name__ == "__main__":
    #lazpartitionsold = createPartitions()
    lazpartitionsnew = createPartitionsnew()
    #if lazpartitionsold != lazpartitionsnew:
     #   print("Not equal")
    #else:
     #   print("Equal")