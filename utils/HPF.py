#! /bin/env python3
'''
   a set of modules for jobs related to HPF.
'''
import sys
import subprocess
from pathlib import Path
from utils.SendEmail import SendEmail

def JsubExitCode (folder, jobid) : 
    """
        return the exitcode:
          -3  => strange error. possibles: status file missing, jobID missing on HPF, jobID killed by HPF etc.
          -2  => the jobs is on hold or on queue
          -1  => still running.
           0  => the job finished successfully.
          >0  => the job failed.
    """

    cmd = "qstat -t %s" % (jobid)
    try:
        status = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, check=True).stdout.decode("utf-8")
    except subprocess.CalledProcessError as grepexc:
        jobname = folder.split('/')[-1]
        fullpathfile = folder + '/' + "status/" + jobname + "." +  str(jobid) + ".status"
        if Path(fullpathfile).is_file():
            cmd = "tail -2 " + fullpathfile + " | head -1 "
            exitcode = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").rstrip()
            if exitcode.startswith('EXIT STATUS:') :
                return int(exitcode.split(' ')[-1])
            else:
                SendEmail("Status warning on jobID %s" % (jobid), "weiw.wang@sickkids.ca", "Status of jobID %s is unknown, neigther can I detect the string'EXIT STATUS' from the file:\n\n  %s !!! \n\nthe job may be killed by the HPF." % (jobid, fullpathfile))
                return -3
        else:
            SendEmail("Status warning on jobID %s" % (jobid), "weiw.wang@sickkids.ca", "Status of jobID %s is unknown, neigther does the file \n\n%s \n\nexist!!! \n\nPlease double check you input carefully." % (jobid, fullpathfile))
            return -3
    else:
        status = status.rstrip().split(' ')[-2]
        if status == 'Q' or status == 'H':
            return -2
        elif status == 'R':
            return -1
        else: 
            jobname = folder.split('/')[-1]
            fullpathfile = folder + '/' + "status/" + jobname + "." +  str(jobid) + ".status"
            if Path(fullpathfile).is_file():
                cmd = "tail -2 " + fullpathfile + " | head -1 "
                exitcode = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").rstrip()
                if exitcode.startswith('EXIT STATUS:') :
                    return int(exitcode.split(' ')[-1])
                else:
                    SendEmail("Status warning on jobID %s" % (jobid), "weiw.wang@sickkids.ca", "Cant't detect the string 'EXIT STATUS' from the file %s.\n If you receive this email from this jobID several time, the HPF may killed this job. " % (fullpathfile, jobid))
                    return -1
            else:
                SendEmail("Status warning on jobID %s" % (jobid), "weiw.wang@sickkids.ca", "Cant't find the status file %s, it is impossible!!! \n\n please check the jobID %s status manually." % (fullpathfile, jobid))
                return -3
                
if __name__ == '__main__':
    print(JsubExitCode(sys.argv[1], sys.argv[2]))

