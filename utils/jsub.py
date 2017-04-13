#! /bin/env python3
'''
   a simple python wrapper for jsub.
   parameters: jobname, command, memory size, #cores, #nodes, walltime, swap disk size, dependances, modules to be loaded
'''
import re
import subprocess
from pathlib import Path
from utils.config import GlobalConfig

def jsub(jobname, command, mem, cpus, nodes='1', wt, ng, depend='', modules):
    jcmd = "echo 'export TMPDIR=/localhd/`echo $PBS_JOBID | cut -d. -f1 ` && \\\n"
    jcmd += "\\\n module load %s  &&  \\\n" % (modules)
    jcmd += "\\\n"
    jcmd += command
    jcmd += "\\\n"
 
    if depend != '':
        depend = '-aft afterok -o ' + str(depend)

    jcmd += "' | jsub -j %s -b %s  -nm %s -np %s -nn %s -nw %s -ng localhd:%s  %s " %(jobname, basefolder, mem, cpus, nodes, wt, str(ng), depend)
    return subprocess.run(jcmd, stdout=subprocess.PIPE, shell=True).stdout.decode("utf-8").split('\n')[0]
