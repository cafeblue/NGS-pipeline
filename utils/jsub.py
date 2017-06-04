#! /bin/env python3
'''
   a simple python wrapper for jsub.
   parameters: jobname, basefolder, command, memory size, #cores, walltime, swap disk space, modules, depend, #nodes to be loaded
'''
import re
import os
import subprocess
from pathlib import Path
from utils.config import GlobalConfig
from utils.SendEmail import SendEmail

def jsub(jobname, basefolder, command, mem, cpus, wt, ng, modules, depend='', nodes='1'):
    if os.path.isdir(basefolder + "/" + jobname):
         SendEmail("deleting a jsub folder....", "weiw.wang@sickkids.ca", basefolder + "/" + jobname)
         subprocess.run("rm -rf %s/%s" % (basefolder, jobname), shell=True)

    jcmd = "echo 'export TMPDIR=/localhd/`echo $PBS_JOBID | cut -d. -f1 ` && \\\n"
    jcmd += "\\\n module load %s  &&  \\\n" % (modules)
    jcmd += "\\\n"
    jcmd += command
    jcmd += "\\\n"
 
    if depend != '':
        depend = '-aft afterok -o ' + str(depend)

    jcmd += "' | jsub -j %s -b %s  -nm %s -np %s -nn %s -nw %s -ng localhd:%s  %s " %(jobname, basefolder, mem, cpus, nodes, wt, str(ng), depend)
    return subprocess.run(jcmd, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").split('\n')[0]
