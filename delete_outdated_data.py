#! /bin/env python3
"""
This script is to delete the sequencing folder rsynced to HPF and some of the running folder.
"""
import sys
from pathlib import Path
import subprocess 
from utils.dbtools import DB_Connector
from utils.TimeString import TimeString
from utils.config import GlobalConfig
from datetime import datetime, date, time, timedelta

class Usage:
    """

    Delete the sequencing folders older than 7 days.
    Delete the running folder older than 8 weeks. 

        Usage: python3 rsyncIlmnRunDir.py database.cnf
        Example: python3 rsyncIlmnRunDir.py clinicalB.cnf

    """

def delete_seq_folder(folder):
    flist = []
    for x in range(14, 7, -1):
        cmd = "cd " + folder + "; ls -d " + (date.today() - timedelta(x)).strftime('%y%m%d') + "_[DNM]*_????_?????*"
        flist.extend(subprocess.run(cmd, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").rstrip().split('\n'))
    cmd = "cd " + folder + "; rm -rf " + " ".join(flist)
    print(cmd)
    subprocess.run(cmd, shell=True)

def delete_run_folder(folder):
    flist = []
    for x in range(63, 56, -1):
        cmd = "cd " + folder + "; ls -d *-*-" + (date.today() - timedelta(x)).strftime('%Y%m%d') + "??????-*.gp*-b37"
        flist.extend(subprocess.run(cmd, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").rstrip().split('\n'))
    cmd = "cd " + folder + "; rm -rf " + " ".join(flist)
    print(cmd)
    subprocess.run(cmd, shell=True)

def main(name, dbfile):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    config = GlobalConfig(conn)
    timestamp = TimeString()
    timestamp.print_timestamp()
    delete_seq_folder(getattr(config, 'SEQ_BACKUP_FOLDER'))
    delete_run_folder(getattr(config, 'HPF_RUNNING_FOLDER'))

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)

