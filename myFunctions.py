"""This file executes two main operation
A) to check if there are polygon without label
B) to seek file netCDF in local storage/internet of specific date"""
from shapely.geometry import Point, Polygon
from dbMongo import *
from sys import exit
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as po
from matplotlib.collections import PatchCollection
import numpy as np
import csv
from netCDF4 import Dataset
import os
import urlparse
import os
import sys
import wget
import datetime

#insert your path
localPath = "<insert your path where are netcdf files>"

url = "http://193.205.230.6:8080/opendap/hyrax/opendap/wrf5"


"""Step 1:
A) to check if DB is actived
B) to check if labels exists
C) to check if polygons exists
D) to check if every polygon has its label
"""
try:
    managedb1 = ManageDB()
except Exception as e:
    print("error: DB not ready")
    sys.exit(0)
result,cursorLabelCollection = managedb1.listLabelCollectionDB()
if(result != True):
   print("Error: there aren't label")
   sys.exit(0)
result1,cursorClassPolygon = managedb1.groupByClassPolygonDB()
if(result1!=True):
    print("Error: there aren't polygon")
    sys.exit(0)
for polygon in cursorClassPolygon:
    #print(polygon)
    if(polygon["_id"]["name"] ==""):
        print("----------------------------------------")
        print("Error: there are polygons without its label")
        print("list of polygon without its label")
        name = polygon["_id"]["name"]
        cursorPolygon = managedb1.getPolygonOnName(name)
        for pol in cursorPolygon:
            print("id",pol["_id"],"datetime",pol["properties"]["dateStr"])
        sys.exit(0)
managedb1.client.close()

"""Step 2:
The MyTool class allows to seek and storage netCDF of specific date
"""

class MyTool(object):

    def __init__(self):
        """This is constructor"""
        pass

    def getUrls(self,rStart,rEnd,yStart,yEnd,mStart,mEnd,dStart,dEnd,hStart,hEnd):
        """this method allows to create a specific path and seeks this one on local storage or internet"""
        urls = []
        ncfiles = []
        print("-------------")
        for r in range(rStart, rEnd+1):  # resolution
            r1 = "d0" + str(r)
            # print(r1)
            for y in range(yStart, yEnd+1):  # year
                for m in range(mStart, mEnd+1):  # month
                    if m < 10:
                        m1 = "0" + str(m)
                    else:
                        m1 = str(m)
                    for d in range(dStart, dEnd+1):  # day
                        if d < 10:
                            d1 = "0" + str(d)
                        else:
                            d1 = str(d)
                        for h in range(hStart, hEnd+1):  # hour
                            if h < 10:
                                h1 = "0" + str(h)
                            else:
                                h1 = str(h)
                            url1 = url + "/" + r1 + "/" + "archive" + "/" + str(
                                y) + "/" + m1 + "/" + d1 + "/" + "wrf5_" + r1 + "_" + str(
                                y) + m1 + d1 + "Z" + h1 + "00" + ".nc"
                            fname = "wrf5_" + r1 + "_" + str(y) + m1 + d1 + "Z" + h1 + "00" + ".nc"
                            print(url1)
                            # http://193.205.230.6:8080/opendap/hyrax/opendap/wrf5/d01/archive/2018/08/01/wrf5_d01_20180801Z0000.nc
                            print(fname)
                            localPath1 = localPath + "/" + r1
                            localPath2 = localPath1 + "/" + fname
                            print(localPath2)
                            if not os.path.isfile(localPath2):
                                print("file doesn't exist in local storage: " + fname)
                                try:
                                    nc = Dataset(url1)
                                    nc.close()
                                    urls.append(url1)
                                    # filename = wget.download(url1,"/Users/rosarioriccio/PycharmProjects/prova1/d01")
                                    # print(filename)
                                    # print("file salvato in locale: " + fname)
                                    print("file exists online: " + fname)
                                except Exception as e:
                                    print("file doesn't exists online")
                            else:
                                print("file exists in local storage: " + fname)
                                ncfiles.append(localPath2)
        return ncfiles,urls

    def getPolygon(self,date1,managedb):
        """this method gets the polygons of specific date"""
        polygons = []
        typePolygons = []
        try:
            cursorPolygon = managedb.getPolygonOnDate(date1)
            for polygon in cursorPolygon:
                coords = []
                print("Info of polygon:")
                print(polygon)
                #print(polygon["properties"]["name"].upper())
                #print(polygon["geometry"]["coordinates"])
                #print("lunghezza",len(polygon["geometry"]["coordinates"][0]))
                for i in range(0,len(polygon["geometry"]["coordinates"][0])):
                    #print("lng:",polygon["geometry"]["coordinates"][0][i][0],"lat:",polygon["geometry"]["coordinates"][0][i][1])
                    coords.append(tuple((polygon["geometry"]["coordinates"][0][i][0],polygon["geometry"]["coordinates"][0][i][1])))
                typePolygons.append(self.assignTypeStorm(managedb,polygon))
                print("class of polygon:",typePolygons[-1])
                #print("geometric polygon")
                #print(coords)
                poly = Polygon(coords)
                #print(poly)
                polygons.append(poly)
            #print("Number of polygon saved on DB n.",len(polygons))
            return polygons,typePolygons
        except Exception as e:
            print("exception",e)
            return None,None

    def assignTypeStorm(self,managedb,polygon):
        """This method associates a specific value to label"""
        result,cursorLabelCollection1 = managedb.listLabelCollectionDB()
        if(result != True):
           print("Error: there aren't labels")
           sys.exit(0)
        name = polygon["properties"]["name"].upper()
        for label in cursorLabelCollection1:
            name1 = label["labelName"].upper()
            #print(name,name1)
            if(name == name1):
                #print("assignment",name,name1)
                labelId = label["labelId"]
                #print("type",labelId)
                return labelId



