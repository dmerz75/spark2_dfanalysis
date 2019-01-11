#!/usr/bin/env python
import os
import sys
import re
from glob import glob
print(sys.version)

# usage:
# ./convert.py -d incomplete/

# mylib:
my_library = os.path.expanduser('~/.myconfigs')
sys.path.append(my_library)

# libraries:
# from mylib.FindAllFiles import *
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
    parser.add_argument("-o","--outdir",help="output directory")
    parser.add_argument("-f","--nfile",help="notebook file")
    args = vars(parser.parse_args())
    return args
args = parse_arguments()

my_dir = os.path.dirname(os.path.abspath('__file__'))



# if 'subdir' in args:
try:
    dirname = args['subdir']
    fp_dirname = os.path.join(my_dir,dirname)
    lst_files = glob(os.path.join(fp_dirname,'*.ipynb'))
except:
    try:
        lst_files = [os.path.join(my_dir,args['nfile'])]
    except:
        pass

# if 'nfile' in args:
# try:

outdir = args['outdir']
fp_outdir = os.path.join(my_dir,outdir)

# print("my_dir:",my_dir)
# print("in-dir:",fp_dirname)
# print("out-dir:",fp_outdir)
# sys.exit()


# print(lst_files)

for nb_file in lst_files:
    # print(nb_file)
    run_command(['jupyter','nbconvert',nb_file])
    html_file = re.sub('ipynb','html',nb_file)
    md_filename = re.sub('html','md',os.path.basename(html_file))
    dest_file = os.path.join(fp_outdir,md_filename)

    print(nb_file)
    print(html_file)
    print(dest_file)

    with open(html_file) as fp:
        text = fp.read()

    os.remove(html_file)

    with open(dest_file,"w") as fpo:
        fpo.write(text)

    # break
