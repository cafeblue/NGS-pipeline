#! /bin/env python3
'''
   a set of modules for jobs related to HPF.
'''
import sys
import subprocess
from pathlib import Path

def JsubExitCode (folder, jobid) : 
    code = "File not found!"
    jobname = folder.split('/')[-1]
    fullpathfile = folder + '/' + "status/" + jobname + "." +  str(jobid) + ".status"
    if Path(fullpathfile).is_file():
        cmd = "tail -2 " + fullpathfile + " | head -1 "
        return subprocess.run(cmd, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").rstrip().split(' ')[-1]
    else:
        return code
