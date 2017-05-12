#! /bin/env python3
"""
This script is to parse the interOp information and dump them into table thing1JobStatus.
"""
import sys
from interop import py_interop_run_metrics, py_interop_run, py_interop_summary
from utils.dbtools import DB_Connector 

def parseInterOp(conn, run_folder, flowcellID):
    run_metrics = py_interop_run_metrics.run_metrics()
    run_folder = run_metrics.read(run_folder)
    summary = py_interop_summary.run_summary()
    py_interop_summary.summarize_run_metrics(run_metrics, summary)

    # read 1
    readsClusterDensity = ",".join(str(int(round(summary.at(0).at(i).density().mean()/1000.0))) + "+/-" + str(int(round(summary.at(0).at(i).density().stddev()/1000.0))) for i in range(summary.lane_count()))
    perReadsPassingFilter = ",".join(str(round(summary.at(0).at(i).reads_pf()/1000000.0, 2)) for i in range(summary.lane_count()))
    perQ30Score = ",".join(str(round(summary.at(0).at(i).percent_gt_q30(), 2)) for i in range(summary.lane_count()))
    numTotalReads = ",".join(str(round(summary.at(0).at(i).reads()/1000000.0, 2)) for i in range(summary.lane_count()))
    aligned = ",".join(str(round(summary.at(0).at(i).percent_aligned().mean(), 2)) + "+/-" + str(round(summary.at(0).at(i).percent_aligned().stddev(), 2)) for i in range(summary.lane_count()))
    errorRate = ",".join(str(round(summary.at(0).at(i).error_rate().mean(), 2)) + "+/-" + str(round(summary.at(0).at(i).error_rate().stddev(), 2)) for i in range(summary.lane_count()))
    clusterPF = ",".join(str(round(summary.at(0).at(i).percent_pf().mean(), 2)) + "+/-" + str(round(summary.at(0).at(i).percent_pf().stddev(), 2)) for i in range(summary.lane_count())) 

    # read 2
    readsClusterDensity += "," + ",".join(str(int(round(summary.at(2).at(i).density().mean()/1000.0))) + "+/-" + str(int(round(summary.at(2).at(i).density().stddev()/1000.0))) for i in range(summary.lane_count()))
    perReadsPassingFilter += "," + ",".join(str(round(summary.at(2).at(i).reads_pf()/1000000.0, 2)) for i in range(summary.lane_count()))
    perQ30Score += "," + ",".join(str(round(summary.at(2).at(i).percent_gt_q30(), 2)) for i in range(summary.lane_count()))
    numTotalReads += "," + ",".join(str(round(summary.at(2).at(i).reads()/1000000.0, 2)) for i in range(summary.lane_count()))
    aligned += "," + ",".join(str(round(summary.at(2).at(i).percent_aligned().mean(), 2)) + "+/-" + str(round(summary.at(2).at(i).percent_aligned().stddev(), 2)) for i in range(summary.lane_count()))
    errorRate += "," + ",".join(str(round(summary.at(2).at(i).error_rate().mean(), 2)) + "+/-" + str(round(summary.at(2).at(i).error_rate().stddev(), 2)) for i in range(summary.lane_count()))
    clusterPF += "," + ",".join(str(round(summary.at(2).at(i).percent_pf().mean(), 2)) + "+/-" + str(round(summary.at(2).at(i).percent_pf().stddev(), 2)) for i in range(summary.lane_count())) 
 
    print("UPDATE thing1JobStatus SET `readsClusterDensity` = '%s', clusterPF = '%s', `numTotalReads` = '%s', `perReadsPassingFilter` = '%s', `perQ30Score` = '%s', aligned = '%s', `ErrorRate` = '%s' WHERE flowcellID = '%s'" % (readsClusterDensity, clusterPF, numTotalReads, perReadsPassingFilter, perQ30Score, aligned, errorRate, flowcellID))
    conn.Execute("UPDATE thing1JobStatus SET `readsClusterDensity` = '%s', clusterPF = '%s', `numTotalReads` = '%s', `perReadsPassingFilter` = '%s', `perQ30Score` = '%s', aligned = '%s', `ErrorRate` = '%s' WHERE flowcellID = '%s'" % (readsClusterDensity, clusterPF, numTotalReads, perReadsPassingFilter, perQ30Score, aligned, errorRate, flowcellID))

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("")
        print("    Usage: python3 %s dbfile runfolder flowcellID" % (sys.argv[0]))
        print("    Example: python3 %s ~/clinicalC.ctf /hpf/largeprojects/pray/AUTOTESTING/runs/170407_D00744_0439_BHHHWCBCXY BHHHWCBCXY" % (sys.argv[0]))
        print("")
        sys.exit(2)

    conn = DB_Connector(sys.argv[1])
    parseInterOp(conn, sys.argv[2], sys.argv[3])
