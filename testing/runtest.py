#!/usr/bin/env python
import argparse
import subprocess
import glob
import shlex
import os.path

parser = argparse.ArgumentParser(description="Run cuckoohash unit tests")
parser.add_argument('files', type=str, nargs='*', help='Run only the tests passed in as arguments (by default, run all .out files in the current directory)', default=[])
parser.add_argument('--filter', type=str, help='Run additionally the tests listed in the given file', default=None)

args = parser.parse_args()
if args.filter:
    tests = open(args.filter).read().split('\n') + args.files
elif len(args.files) == 0:
    tests = glob.glob('*.out')
else:
    tests = args.files

for t in tests:
    print 'RUNNING', t
    try:
        command = shlex.split(t)
        subprocess.call([os.path.abspath(command[0])] + command[1:])
    except Exception as e:
        print e
