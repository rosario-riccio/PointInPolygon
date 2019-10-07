"""This file rapresents the main of software: its purpose is to analyze every point of netCDF file to establish
 if this one belonged to specific polygon or not; this info will be stored in a csv file"""

from myFunctions import *
from worker import *
from netCDF4 import Dataset
import numpy
import os
from glob import glob
import datetime
import multiprocessing
import time

import urlparse
import os
import sys

def main():
    #print("Main")
    nThreads = multiprocessing.cpu_count()
    print("Number of threads",nThreads)
    mytool = MyTool()
    #resolution, start date, end date
    ncfiles,urls = mytool.getUrls(1,1,2018,10,29,0,2018,10,29,2)
    print("------------------------------------------------------------------------------------")
    print("number of file in local storage:",len(ncfiles),"number of file in internet:",len(urls))
    p = multiprocessing.Pool(processes=nThreads)
    print("------------------------------------------------------------------------------------")
    print("Start scan files online")
    p.map(workerUrls,urls)
    print("------------------------------------------------------------------------------------")
    print("Start scan files in local storage")
    #The software divides the analysis of netCDF files amoung threads
    p.map(workerNcfile,ncfiles)

if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print("--- %s seconds ---" % (start - end))
