#! /bin/env python3
"""
This script is to find the latest runfolders on the sequencers and create a new row in table thing1JobStatus for each flowcellID.
"""
import sys
import re
from pathlib import Path
import subprocess 
from utils.dbtools import DB_Connector, CronControlPanel, getActiveRunFolders
from utils.sequencers import getRunInfo
from utils.common import TimeString
from utils.config import GlobalConfig
from utils.SendEmail import SendEmail

class Usage:
    """
        Find the latest runfolders on the sequencers and create a new row in table thing1JobStatus for each flowcellID.
        
        Usage: python3 rsyncIlmnRunDir.py database.cnf
        Example: python3 rsyncIlmnRunDir.py clinicalB.cnf

    """

def passInfo(folder, config, conn):
    runinfo = getRunInfo(folder + '/' + getattr(config, 'SEQ_RUN_INFO_FILE'))
    flowcellID = folder.split('_')[-1].upper()
    machine,   = re.findall('(?<=/sequencers/).+?/', folder)
    machine    = re.sub(r'/', r'', machine)

    if "NumCycles" in runinfo.keys():
        msg = UpdateDatabase(conn, flowcellID, machine, folder, getattr(config, 'RUN_BACKUP_FOLDER') + folder.split('/')[-1], runinfo)
        SendEmail( "Sequencing folder for " + flowcellID + " found.", "weiw.wang@sickkids.ca", msg)
    else:
        SendEmail(flowcellID + " Error", "weiw.wang@sickkids.ca", "Failed to check the cyclenumbers or cycle number equal to 0?")

def UpdateDatabase(conn, flowcellID, machine, folder, destfolder, runinfo):
    if len(conn.Execute("SELECT * from thing1JobStatus where flowcellID = '" + flowcellID + "'")) > 0:
        return flowcellID + " already exists in the table thing1JobStatus, please check if there are two or more running folders of this flowcell on the sequencer.\n"
    else:
        conn.Execute("INSERT INTO thing1JobStatus ( flowcellID, machine, rundir, destinationDir, sequencing, cycleNum, LaneCount, SurfaceCount, SwathCount, TileCount, SectionPerLane, LanePerSection ) VALUES ('%s', '%s', '%s', '%s', '2', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (flowcellID, machine, folder, destfolder, str(runinfo['NumCycles'][0] + runinfo['NumCycles'][1] + runinfo['NumCycles'][2]), str(runinfo['LaneCount']), str(runinfo['SurfaceCount']), str(runinfo['SwathCount']), str(runinfo['TileCount']), str(runinfo['SectionPerLane']), str(runinfo['LanePerSection'])))
        return "As Subject"

def main(name, dbfile):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    config = GlobalConfig(conn)
    cron_control = CronControlPanel(conn)
    runningFolders = set(cron_control.get_rf().rstrip().split('\n'))

    command = 'find %s  -maxdepth 1 -name "??????_[DNM]*_????_*" -mtime -1 ' %  (getActiveRunFolders(conn))
    todayFolders = set(subprocess.run(command, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").rstrip().split('\n'))

    folders = todayFolders - runningFolders;
    if len(folders) == 0:
        sys.exit(1)

    timestamp = TimeString()
    timestamp.print_timestamp()
    for folder in folders:
        passInfo(folder, config, conn)
    cron_control.update_rf('\n'.join(list(todayFolders)) + '\n')


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
