#! /bin/env python3
"""
This script will do the following jobs:
    1. check the demultiplex status
    2. if done, rename the fastq files accordingly.
    3. parse the demultiplex information and upate the table thing1JobStatus in the database.
"""

import sys
import os
import re
from pathlib import Path
from utils.dbtools import DB_Connector 
from utils.common import TimeString
from utils.config import GlobalConfig
from utils.SendEmail import SendEmail
from utils.HPF import JsubExitCode

class Usage:
    """
        Rename the fastq files, parse the demultiplex info and update the table thing1JobStatus..

        Usage: python3 post_demultiplex.py database.cnf
        Example: python3 post_demultiplex.py clinicalB.cnf

    """

def main(name, dbfile):

    if Path(dbfile).is_file():
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    config = GlobalConfig(conn)




if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)

