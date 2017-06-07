#! /bin/env python3
"""
This script is to push through the sample failed to pass the QC but manually reviewed 
"""
import sys
from pathlib import Path
from utils.dbtools import DB_Connector

class Usage:
    """

    Change the currentStatus from ? to 0. (restart a sample)

        Usage: python3 restart_pipeline4sample.py database.cnf postprocID
        Example: python3 restart_pipeline4sample.py clinicalB.cnf 5069

    """

def main(name, dbfile, ppid):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)

    checkS = conn.Execute("SELECT currentStatus FROM sampleInfo WHERE postprocID = '%s'" %(ppid))
    if checkS[0]['currentStatus'] % 2 == 0:
        print("the currentStatus is supposed to an odd number, but it is %s, ignored" % (checkS[0]['currentStatus']))
        sys.exit(0)
    conn.Execute("UPDATE sampleInfo SET currentStatus = '0' WHERE postprocID = '%s'" % (ppid))

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
