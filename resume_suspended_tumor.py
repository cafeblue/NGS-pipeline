#! /bin/env python3

""" A script to resume a tumor sample which is suspended due to the normal pair has not finished."""

import sys
import os
import re
from pathlib import Path
from utils.dbtools import DB_Connector 
from utils.SendEmail import SendEmail

class Usage:
    """ 
    This script is to resume a tumor sample which is suspended due to the normal pair had not finished, 
        or the normal pair did not exist when it was submitted a few hours ago.

        Usage: python3 resume_suspended_tumor.py db.confg 
        Example: python3 resume_suspended_tumor.py clinicalC.cfg 
    """

def main(name, dbfile):

    if Path(dbfile).is_file(): 
        pass
    else:
        print("file: " + str(dbfile) + " does not exists!")
        sys.exit(2)

    conn = DB_Connector(dbfile)
    idle_tumors = conn.Execute("SELECT sampleID, postprocID, pairID, TIMESTAMPDIFF(HOUR, time, NOW()) hours FROM sampleInfo WHERE currentStatus = '1'")
    if len(idle_tumors) == 0:
        sys.exit(0)

    email_content = ""
    for tumor in idle_tumors:
        normal_pair = conn.Execute("SELECT sampleID,postprocID,currentStatus FROM sampleInfo WHERE pairID = '%s' AND genePanelVer = 'cancer.gp19' AND sampleType = 'normal' ORDER BY postprocID DESC LIMIT 1" % (tumor['pairID']))
        if len(normal_pair) == 0:
            email_content += "Tumor sample (sampleID %s postprocID %s) has been waiting over %s hours, but NO normal pair can be found for pairID %s. \n" % (tumor['sampleID'], tumor['postprocID'], tumor['hours'], tumor['pairID'])
            continue

        normal = normal_pair[0]
        if normal['currentStatus'] < 6:
            email_content += "Tumor sample (sampleID %s postprocID %s) has been waiting over %s hours, but the normal pair (sampleID %s postprocID %s currentStatus %s) is still running or failed.\n" % (tumor['sampleID'], tumor['postprocID'], tumor['hours'], normal['sampleID'], normal['postprocID'], normal['currentStatus'])
            continue

        #print("DELETE FROM hpfJobStatus WHERE postprocID = '%s'" % (tumor['postprocID']))
        conn.Execute("DELETE FROM hpfJobStatus WHERE postprocID = '%s'" % (tumor['postprocID']))
        #print("UPDATE sampleInfo SET currentStatus = '0' WHERE postprocID = '%s'" % (tumor['postprocID']))
        conn.Execute("UPDATE sampleInfo SET currentStatus = '0' WHERE postprocID = '%s'" % (tumor['postprocID']))

    if email_content != '':
        SendEmail("Re-Submission Failed", "weiw.wang@sickkids.ca", email_content) 

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)

