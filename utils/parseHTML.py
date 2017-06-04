#! /bin/env python3
'''
   parse the html output from bcl2fastq
'''
import sys
import subprocess
import collections
from pathlib import Path
#from SendEmail import *
from bs4 import BeautifulSoup
import pandas as pd
#from utils.SendEmail import *

def parseDemultiplexTable (demultiplexhtml, mycolumns=[1, 3, 4, 7, 9], tindex = 2) : 
    """
        Return a dict which contain the demultiplex information for each sample.
        parameter1:  html file
        parameter2:  column range e.g. [0,1,3,4,5,7,9]
        parameter3:  table index, an integer. which table in the html. 
    """
    with open(demultiplexhtml) as fp:
        soup = BeautifulSoup(fp, 'lxml') # Parse the HTML as a string
        table = soup.find_all('table')[tindex] 

        demuDict = {}
        returnDict = collections.defaultdict(dict)
        for row in table.find_all('tr'):
            cont = ""
            columns = row.find_all('td')
            if len(columns) == 0:
                continue

            cont = [columns[idx].get_text().replace(",", "") for idx in mycolumns]
            if cont[0] in demuDict:
                demuDict[cont[0]].append(cont[1:])
            else:
                demuDict[cont[0]] = [cont[1:]]
        for sampleid,tmplist in demuDict.items():
            returnDict[sampleid]['numReads']    = sum(int(tmplist[x][0]) for x in range(len(tmplist)))
            returnDict[sampleid]['perIndex']    = '%5.2f' % (sum(float(tmplist[x][1]) for x in range(len(tmplist)))/float(len(tmplist)))
            returnDict[sampleid]['yieldMB']     = sum(int(tmplist[x][2]) for x in range(len(tmplist)))
            returnDict[sampleid]['perQ30Bases'] = '%5.2f' % (sum(float(tmplist[x][3]) for x in range(len(tmplist)))/float(len(tmplist)))
        return returnDict
                
if __name__ == '__main__':
    print(dict(parseDemultiplexTable(sys.argv[1])))

