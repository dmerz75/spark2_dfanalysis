#!/usr/bin/env python
import os
import sys
from glob import glob
print(sys.version)

# mylib:
my_library = os.path.expanduser('~/.myconfigs')
sys.path.append(my_library)

# libraries:
from mylib.FindAllFiles import *
# from mylib.moving_average import *
# from mylib.cp import *
# from mylib.FindAllFiles import *
# from mylib.highway_check import *
# from mylib.moving_average import *
# from mylib.regex import reg_ex
from mylib.run_command import run_command

my_dir = os.path.dirname(os.path.abspath('__file__'))
print(my_dir)

lst_files = glob.glob(os.path.join(my_dir,'*.ipynb'))
# print(lst_files)

for nb_file in lst_files:
    # print(nb_file)
    run_command(['jupyter','nbconvert',nb_file])
