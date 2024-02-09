import sys

import pandas as pd
print(sys.argv)


day = sys.argv[1]

print(f'We used pip to install Pandas {pd.__version__}')
print('Job successfully completed for {}'.format(day))