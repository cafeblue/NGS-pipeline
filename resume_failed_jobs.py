#! /bin/env python3

""" A script to resume a pipeline from a checkpoint."""

import sys
import os
import re
from pathlib import Path
from utils.dbtools import DB_Connector 

class Usage:
    """ 
    This script is to resume a pipeline from a checkpoint on a postprocID:

        Usage: python3 resume_failed_jobs.py db.confg postprocID checkpoint
        Example: python3 resume_failed_jobs.py clinicalC.cfg 5020 picardMarkDup
    """

def main(name, dbfile, ppid, checkpoint):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("file: " + str(dbfile) + " does not exists!")
        sys.exit(2)

    command = DB_Connector(dbfile)
    command = command.Execute("SELECT command FROM hpfCommand WHERE postprocID = '%s'" % (str(ppid)))
    if len(command) != 1:
        print("rows of command for ppID: $s are more than one, it is impossible!!!" % (str(ppid)))
        sys.exit(2)

    command = re.sub(r'"$', r' -i %s "' % (checkpoint), command[0]['command'])
    print('====================')
    print(command)
    print('====================')
    os.system(command)

    update = DB_Connector(dbfile)
    update.Execute("UPDATE hpfJobStatus SET jobID = NULL, exitcode = NULL, flag = NULL WHERE postprocID = '%s'" % (str(ppid)))
    update.Execute("UPDATE sampleInfo SET currentStatus = '2' WHERE postprocID = '%s'" % (str(ppid)))

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
