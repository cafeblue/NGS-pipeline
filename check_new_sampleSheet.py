#! /bin/env python3

import os,sys
import pickle

def main(database_cfg):
    if os.path.exists(database_cfg) :
        print(database_cfg + " exists!", file=sys.stderr)
    else : 
        print(database_cfg + " can't be found!", file=sys.stderr)

    dbcfg_fh = open(database_cfg, 'rb')
    db = pickle.load(dbcfg_fh)


if __name__ == "__main__":
    main(sys.argv[1])
