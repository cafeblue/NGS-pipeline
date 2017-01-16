#! /bin/env python3

import datetime

today = datetime.datetime.now() 
yesterday = today - 86400

print("\n\n_/ _/ _/ _/ _/ _/ _/ _/\n  {:%Y-%m-%d %H:%M:%S}\n_/ _/ _/ _/ _/ _/ _/ _/\n".format(today))
print("\n\n_/ _/ _/ _/ _/ _/ _/ _/\n  {:%Y-%m-%d %H:%M:%S}\n_/ _/ _/ _/ _/ _/ _/ _/\n".format(today), file=sys.stderr)




