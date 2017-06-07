#! /bin/env python3
"""
This script is to push through the sample failed to pass the QC but manually reviewed 
"""
import sys
import re
from pathlib import Path
import subprocess 
from utils.dbtools import DB_Connector, CronControlPanel
from utils.TimeString import TimeString
from utils.SendEmail import SendEmail

class Usage:
    """

    Change the currentStatus from 7 to 6. (Push the low QC sample through)

        Usage: python3 push_LowQual_through.py database.cnf postprocID
        Example: python3 push_LowQual_through.py clinicalB.cnf 5069

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
    if checkS[0]['currentStatus'] != 7:
        print("the currentStatus is supposed to be 7, but it is %s, ignored" % (checkS[0]['currentStatus']))
        sys.exit(0)

    conn.Execute("UPDATE sampleInfo SET currentStatus = '6' WHERE postprocID = '%s'" % (ppid))

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)

