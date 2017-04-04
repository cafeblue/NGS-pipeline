#! /bin/evn python3
"""
    insert job list into table hpfJobStatus according to the sampleID, ppid and pipeID 
"""

import os
import sys
import pickle
from pathlib import Path
import subprocess 
from utils import *

class Usage:
    """

        Usage: python3 rsyncIlmnRunDir.py database.cnf postprocID pipeID
        Example: python3 rsyncIlmnRunDir.py clinicalB.cnf 2787 ilmn.cr.p5.1

    """

def main(name, dbfile, ppid, pipeid):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    jobstring = conn.Execute("SELECT steps from pipelineHPF where pipeID = '" + pipeid + "'")
    joblist = jobstring[0]['steps']
    joblist = joblist.split(r",")
    sampleid = conn.Execute("SELECT sampleID from sampleInfo where postprocID = '" + ppid + "'")[0]['sampleID']
    print(sampleid)
    for job in joblist :
        conn.Execute("INSERT INTO hpfJobStatus (sampleID, postprocID, jobName) VALUES ('" + sampleid + "', '" + ppid + "', '" + job + "')")
        #print("INSERT INTO hpfJobStatus (sampleID, postprocID, jobName) VALUES ('" + sampleid + "', '" + ppid + "', '" + job + "')")

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)

