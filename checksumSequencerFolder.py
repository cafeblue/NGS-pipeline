#! /bin/evn python3
"""
    check the chksum of the folder on sequencer and the folder rsynced to Thing1.
"""

import os
import sys
import pickle
from pathlib import Path
import subprocess 
from utils import *

class Usage:
    """

    Read the records in table thing1JobStatus which seqFolderChksum = '2'(means sequencing finished), and then checksum the folder.

        Usage: python3 checksumSequencerFolder.py database.cnf
        Example: python3 checksumSequencerFolder.py clinicalB.cnf

    """
def checksumsequenerfolder(conn):
    folders = conn.Execute("SELECT flowcellID,machine,runDir,destinationDir from thing1JobStatus where seqFolderChksum = '2'");
    if len(folders) != 0 :
        timestamp = TimeString()
        timestamp.print_timestamp()
        config = GlobalConfig(conn)
        content = ""
        for row in folders :
            command = 'cd %s ; find . -type f \( ! -iname "IndexMetricsOut.bin" ! -iname "*omplete*" \) -print0 | xargs -0 sha256sum > %s%s.%s.sha256\n' % ( row['destinationDir'], getattr(config, "RUN_CHKSUM_DIR"), row['machine'], row['flowcellID'])
            try:
                subprocess.check_call(command, shell=True)
            except CalledProcessError:
                content += 'Failed to checksum runfolder on sequencer for flowcell %s\n' %(row['flowcellID'])
                continue

            command = "cd %s ; sha256sum -c %s%s.%s.sha256 | grep -i failed" % (row['runDir'], getattr(config, "RUN_CHKSUM_DIR"), row['machine'], row['flowcellID'])
            try:
                subprocess.check_call(command, shell=True)
            except CalledProcessError:
                content += "chksums of the flowcell from the sequencer folder: %s and the folder on thing1: %s are not identical!\n" % (row['rundir'], row['destinationDir'])

        if content != "" :
            SendEmail("chksum failed.", "weiw.wang@sickkids.ca", content)

def main(name, dbfile):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    cron_control = CronControlPanel(conn)
    cron_control.start_process('chksum_sequencer')
    checksumsequenerfolder(conn)
    cron_control.stop_process('chksum_sequencer')

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)

