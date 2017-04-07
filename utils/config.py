#! /bin/env python3
class GlobalConfig():
    """ Read in all the config from table config in the database."""
    def __init__(self, conn):
        self.rows = conn.Execute('SELECT * FROM config')
        setattr(self, "", "")
        for row in self.rows:
            setattr(self, row['vName'], row['vValue'])

class Sequencers():
    """Read in all the config of Sequencers"""
    def __init__(self, conn):
        self.active_seq = conn.Execute("SELECT * FROM sequencers WHERE active = '1'")
        for row in self.active_seq:
            setattr(self, row['machine'], row)

