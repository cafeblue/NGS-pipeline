#! /bin/env python3
import sys
from datetime import datetime, date, time

class TimeString():
    """ generate a string of the date/time"""

    def __init__(self):
        self.timestring = datetime.now()
        self.fulltime = self.timestring.strftime("%Y%m%d%H%M%S")
        self.dateslash = self.timestring.strftime('%m/%d/%Y')
        self.longdate = self.timestring.strftime('%Y%m%d')
        self.yesterday = self.timestring.replace(day=self.timestring.day-1).strftime('%Y%m%d')
        self.timestamp = self.timestring.strftime("%Y-%m-%d %H:%M:%S")

    def print_timestamp(self):
        print("\n\n_/ _/ _/ _/ _/ _/ _/ _/\n  " + self.timestamp + "\n_/ _/ _/ _/ _/ _/ _/ _/\n")
        print("\n\n_/ _/ _/ _/ _/ _/ _/ _/\n  " + self.timestamp + "\n_/ _/ _/ _/ _/ _/ _/ _/\n", file=sys.stderr)
