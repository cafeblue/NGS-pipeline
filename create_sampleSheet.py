#! /bin/env python3

"""
    This script is to read the latest records in table sampleSheet and generate the samplesheet for the sequencer.
"""


import sys
from pathlib import Path
from utils.dbtools import DB_Connector
from utils.common import TimeString
from utils.sequencers import SampleSheet

class Usage:
    """

    Program to read the new records in table sampleSheet and create the sampleSheet for sequencer.

        Usage: python3 read_new_sampleSheet_rows.py database.cnf
        Example: python3 read_new_sampleSheet_rows.py clinicalB.cnf

    """


def main(name, dbfile):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    newRows = conn.Execute("SELECT * FROM sampleSheet WHERE TIMESTAMPADD(SECOND,60,time) > NOW()")

    if len(newRows) == 0 :
        sys.exit(0)

    time_obj = TimeString()
    time_obj.print_timestamp()

    samplesheet = SampleSheet(newRows, conn, time_obj)
    samplesheet.seq_samplesheet()

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
