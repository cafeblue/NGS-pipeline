#! /bin/env python3

"""
    This script is to update the pairedSampleID/sampleType in table sampleSheet
"""


import sys
import re
from pathlib import Path
from utils.dbtools import DB_Connector

class Usage:
    """

Program to read in a table and update the sampleSheet table in the database

    Usage:    python3 update_pairID.py db.cfg flowcell
    Example:  python3 update_pairID,py ~/clinicalC.cfg ~/BHKFLCBCXY

The second file should be named as the flowcellID and the content should be the format like below:

    sampleID1\\tpairID\\tsample_type

Example:
    202214	110	tumor
    201192	110	normal

    """    

def main(name, dbfile, flowcellfile):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    if Path(flowcellfile).is_file(): 
        pass
    else:
        print("ERR: file: " + str(flowcellfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    flowcell = Path(flowcellfile).name

    #check flowcellID
    newRows = conn.Execute("SELECT sampleID,sample_type,time FROM sampleSheet WHERE flowcell_ID = '%s'" % (flowcell))
    if len(newRows) == 0 :
        print("There is no record in the table sampleSheet which flowcellID equals to %s" % (flowcell))
        print("Please double check the flowcellID")
        sys.exit(0)
    else:
        print("There are %d samples on this flowcell." % (len(newRows)))

    #read the pairID information and check if the sampleID exists in the table
    print("")
    mysqlCMD = ""
    num_update = 0
    for line in open(flowcellfile):
        if not re.match('.+\t.+\t.+', line):
            print("Warning!!!  the following line is ignored:\n%s" % (line))
            continue
            
        sid, pid, stype = line.strip().split('\t')
        for row in newRows:
            if row['sampleID'] == sid:
                num_update += 1
                newstype = stype.lower().replace('tumour', 'tumor')
                if newstype != 'normal' and newstype != 'tumor':
                    print("I don't understand the sample type: %s. it should be 'normal' or 'tumor'" % (stype))
                    print("please fix the third column accordingly.")
                    sys.exit(0)
                
                mysqlCMD += "UPDATE sampleSheet SET  pairedSampleID = '%s', sample_type = '%s', time = '%s' WHERE sampleID = '%s' AND flowcell_ID = '%s'; " % (pid, newstype, row['time'], sid, flowcell)
                break
        else:
            print("sampleID %s is not on this flowcell, please double check!" % (sid))
            sys.exit(0)

    print("There are %d samples in your file." % (num_update))
    print(mysqlCMD.replace("; ", "\n"))
    conn.Execute(mysqlCMD)

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
