#-*- coding: UTF-8 -*-
'''
Tools for computing statistics about run time
compatibility: python 3.X
'''

import sys
import os.path
import argparse
import subprocess
from itertools import groupby
import numpy as np
import scipy.stats as spst

#import cProfile

VERSION = '0.1.0'

dataKeywords = ('real','cpu','user')

def documentation():
    """
    Print detailed documentation.
    """
    print('\n')
    print('DOCUMENTATION')
    print('============='+'\n')
    print('Run & compute elapsed time statistics')
    print('-------------------------------------'+'\n')
    print(' '*2+'SYNTAX')
    print(' '*4+'python pTimeStats.py command "command_name arg1 arg2 ..." NUM'+'\n')
    print(' '*2+'DESCRIPTION')
    print(' '*4+'Runs the command \'command_name\' w/ the specified arguments (arg1, arg2) \'NUM\' times')
    print(' '*4+'and compute statistics related to:')
    print(' '*4+'* the real elapsed time')
    print(' '*4+'* the percentage of total CPU load (100% per core)')
    print(' '*4+'* the user elapsed time')
    print(' '*4+'The computed statistics are: min, mean, max and 90% confidence interval')
    print('\n')

    print('Process a pTimeStats log file')
    print('-----------------------------'+'\n')
    print(' '*2+'SYNTAX')
    print(' '*4+'python pTimeStats.py process-log filename'+'\n')
    print(' '*2+'DESCRIPTION')
    print(' '*4+'Process the time logs the file \'filename\'. The log files are created when using command')
    print(' '*4+'pTimeStats.py command with the --log option. The data in these files stem from the ')
    print(' '*4+'output of the shell command: ')
    print(' '*4+'/usr/bin/time -f "real:%E user:%U,%P cpu:%P" ...')
    print()


def loadTxtFile(filename):
    """
    Load data from a log file with one-line records
    """
    if not(os.path.isfile(filename)):
        raise OSError('Function loadTxtFile(filename): impossible to load \'{0}\', there is no such file.'.format(filename))

    # Read and word split
    with open(filename,'r') as ioObj:
        splitLines = [line.split() for line in ioObj]

    return splitLines

def mapReduce(splitLines):
    # Map to key/value pairs
    def kwdMap(word):
        """
        Map word to (keyword,value) pair
        """
        wordSplit = word.split(':')
        if len(wordSplit)<2:
            return None
        kwd = wordSplit[0].lower()
        val = wordSplit[1:]
        if not(kwd in dataKeywords):
            return(None)
        elif kwd=='cpu':
            try:
                return( ('cpu',float(val[0].replace('%',''))) )
            except:
                return(None)
        else:
            return (kwd,val)


    mappedPairs = filter( lambda x: x!=None, (kwdMap(word) for words in splitLines for word in words) )

    # Reduce by keys
    sortedPairs = sorted(mappedPairs, key=(lambda t: t[0]))
    reducedValues = dict()
    for k,g in groupby(sortedPairs, key=lambda t: t[0]):
        reducedValues[k] = [kv[1] for kv in g]

    return reducedValues



def stats(samples):
    """
    Compute usual statistics
    """
    statistics = dict()

    def timeConverter(timeList):
        """
        timeList = [[hours]:[minutes]:seconds]
        """
        dim = len(timeList)
        return np.sum([float(txt)*60**(dim-1-pos) for pos,txt in enumerate(timeList)])

    def userConverter(val):
        """
        e.g. val is ['7.08,188%']
        ['s.ss,xxx%'] to s.ss/x.xx
        """
        splVal = val[0].split(',')
        return float(splVal[0])/float(splVal[1].replace('%',''))*100.

    def sampler(key,sample):
        """
        Convert sample format for statistical estimator functions
        """
        if key=='cpu':
            return np.array(sample, dtype=float)
        if key=='real':
            return np.array(list(map(timeConverter, iter(sample))), dtype=float)
        if key=='user':
            return np.array(list(map(userConverter, iter(sample))), dtype=float)


    def computeStatistics(smpl):
        """
        Return a dict object with mean, [5percentile, 95percentile], min, max
        """
        # (0.4,0.4) : approximately quantile unbiased (Cunnane)
        quantEst=spst.mstats.mquantiles(smpl, prob=[0.5, 0.95], alphap=0.4, betap=0.4) # 90%-confidence
        return {'mean':np.mean(smpl), '90%-conf':quantEst, 'min':np.min(smpl), 'max':np.max(smpl)}


    return dict( [ (key, computeStatistics(sampler(key,sample))) for (key,sample) in samples.items()] )


def displayStats(statistics):
    """
    Display statistics values to the standard output
    """
    title = {'real':'Elapsed real time',\
    'user':'Total number of CPU-seconds that the process spent in user mode divided by the CPU load',\
    'cpu':'Percentage of a CPU that this job got'}
    unit = {'real':'sec', 'user':'sec', 'cpu':'%'}

    for key in statistics.keys():
        print( (lambda k: title[k] if (k in title.keys()) else k)(key) )
        print('Min value:       {value:.2f} {unit}'.format(value=statistics[key]['min'], unit=unit[key]))
        print('Mean value:      {value:.2f} {unit}'.format(value=statistics[key]['mean'], unit=unit[key]))
        print('Max value:       {value:.2f} {unit}'.format(value=statistics[key]['max'], unit=unit[key]))
        quant = statistics[key]['90%-conf']
        print('90% confidence:  [{0:.2f} , {1:.2f}] {2}'.format(quant[0],quant[1], unit[key]))
        print()


def runSampling(command,runNum):
    """
    """
    subprocArgList = ['/usr/bin/time','-f', '"real:%E user:%U,%P cpu:%P"']+command.split()
    def generate_run(N):
        for n in range(N):
            proc = subprocess.Popen(subprocArgList, stderr=subprocess.PIPE)
            yield proc.stderr.read().decode('UTF-8').replace('\"','').split()

    return list(generate_run(int(runNum)))


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Tools for computing statistics about run time')
    parser.add_argument('taskItem', nargs='*', help="""Label specifying the tools to be called and tasks to be performed
    followed by the proper additional arguments. Use --doc option for more details.""")
    parser.add_argument('--doc', action='store_true', help='Show the documentation')
    parser.add_argument('--version', action='store_true', help='Show version')
    parser.add_argument('--log', action='store_true', help='Create a log file with time results')
    args = parser.parse_args()

    # Show documentation
    if args.doc:
        documentation()
        exit(0)

    # Show version
    if args.version:
        print('Version '+VERSION)
        esit(0)

    try:
        if args.taskItem[0]=='command':
            if len(args.taskItem)!=3:
                sys.stderr.write('Aborted. Three arguments required.'+'\n'*2)
                sys.stderr.write('SYNTAX:\n')
                sys.stderr.write('  pTimeStats.py command "command_name arg1 arg2 ..." runNum\n')
                exit(1)
            command = args.taskItem[1]
            runNum = args.taskItem[2]
            # displayStats( stats( mapReduce( runSampling(command,runNum) ) ) )
            splitLines = runSampling(command,runNum)
            if args.log:
                with open('pTimeStats.log','w') as ioObj:
                    for words in splitLines:
                        ioObj.write(' '.join(words)+'\n')
            displayStats( stats( mapReduce( splitLines ) ) )

        if args.taskItem[0]=='process-log':
            if len(args.taskItem)!=2:
                sys.stderr.write('Aborted. Two arguments required.'+'\n'*2)
                sys.stderr.write('SYNTAX:\n')
                sys.stderr.write('  pTimeStats.py process-log filename\n')
                exit(1)
            displayStats( stats( mapReduce( loadTxtFile( args.taskItem[1]) ) ) )

    except OSError as err:
        print(err)
