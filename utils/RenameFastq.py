#! /bin/env python3
import sys
import os

def RenameFastq(folder, numlane, flowcellID, sampleIDs):
    for idx in range(len(sampleIDs)):
        os.mkdir(folder + "/Sample_" + sampleIDs[idx])
        for lane in range(1, int(numlane) + 1):
            os.rename("%s/%s_S%d_L00%d_R1_001.fastq.gz" % (folder, sampleIDs[idx], idx+1, lane), "%s/Sample_%s/%s_%s_R1_%d.fastq.gz" % (folder, sampleIDs[idx], sampleIDs[idx], flowcellID, lane))
            os.rename("%s/%s_S%d_L00%d_R2_001.fastq.gz" % (folder, sampleIDs[idx], idx+1, lane), "%s/Sample_%s/%s_%s_R2_%d.fastq.gz" % (folder, sampleIDs[idx], sampleIDs[idx], flowcellID, lane))
