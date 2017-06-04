#! /bin/env python3
class GlobalConfig():
    """ Read in all the config from table config in the database."""
    def __init__(self, conn):
        self.rows = conn.Execute('SELECT * FROM config')
        setattr(self, "", "")
        for row in self.rows:
            setattr(self, row['vName'], row['vValue'])

class gpConfig():
    """ Read in all the gpConfig from table gpConfig in the database."""
    def __init__(self, conn):
        self.rows = conn.Execute("SELECT * FROM gpConfig where active = '1'")
        setattr(self, "", "")
        for row in self.rows:
            setattr(self, row['genePanelID'] + "\t" + row['captureKit'], row)

