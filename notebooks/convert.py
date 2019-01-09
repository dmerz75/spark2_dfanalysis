#!/usr/bin/env python
import os
import sys
from glob import glob
print(sys.version)

# usage:
# ./convert.py -d incomplete/

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
# from mylib.myargs import parse_arguments
from mylib.run_command import run_command

#  ---------------------------------------------------------  #
#  argparse                                                   #
#  ---------------------------------------------------------  #
import argparse

def parse_arguments():
    '''
    Parse script's arguments.
    Options:
    args['makefile']
    args['procs']
    args['node'])
    '''
    parser = argparse.ArgumentParser()
    # parser.add_argument("-r","--range",help="range for running")
    parser.add_argument("-d","--subdir",help="subdirectory provided")
    args = vars(parser.parse_args())
    return args
args = parse_arguments()


my_dir = os.path.dirname(os.path.abspath('__file__'))
dirname = args['subdir']
fp_dirname = os.path.join(my_dir,dirname)
print(my_dir)


lst_files = glob.glob(os.path.join(fp_dirname,'*.ipynb'))
# print(lst_files)

for nb_file in lst_files:
    # print(nb_file)
    run_command(['jupyter','nbconvert',nb_file])
