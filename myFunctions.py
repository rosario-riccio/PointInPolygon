"""This file executes two main operation
A) to check if there are polygon without label
B) to seek file netCDF in local storage/internet of specific date"""
from shapely.geometry import Point, Polygon
from dbMongo import *
from datetime import date, timedelta,datetime

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


#insert your path
localPath = "/home/rosario/Scrivania/dataTemp1"
#localPath = "/Users/rosarioriccio/Desktop/dataTemp"
url = "http://193.205.230.6:8080/opendap/hyrax/opendap/wrf5"


"""Step 1:
A) to check if DB was actived
B) to check if labels existed
C) to check if polygons existed
D) to check if every polygon had its label
"""

#Controllo attivazione DB,presenza label,polygon,polygon senza etichetta
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


    def getUrls1(self,rStart,rEnd,yStart,mStart,dStart,hStart,yEnd,mEnd,dEnd,hEnd):
        urls = []
        ncfiles = []
        for r in range(rStart,rEnd+1):
            r1 = "d0" + str(r)
            sdate = datetime(yStart,mStart,dStart,hStart)
            edate = datetime(yEnd,mEnd,dEnd,hEnd)
            delta = int((edate-sdate).total_seconds()/3600)
            for i in range(delta+1):
                tempDate = sdate + timedelta(hours=i)
                time1 = tempDate.strftime("%Y%m%dZ%H%M")
                year = tempDate.strftime("%Y")
                month = tempDate.strftime("%m")
                day = tempDate.strftime("%d")
                url1 = url + "/" + r1 + "/" + "archive" + "/" + year + "/" + month + "/" + day + "/" + "wrf5_" + r1 + "_" + time1 + ".nc"
                fname = time1 + ".nc"
                print(url1)
                print(fname)
                # http://193.205.230.6:8080/opendap/hyrax/opendap/wrf5/d01/archive/2018/08/01/wrf5_d01_20180801Z0000.nc
                localPath1 = localPath + "/" + r1
                localPath2 = localPath1 + "/" + fname
                print(localPath2)
                if not os.path.isfile(localPath2):
                    print("file non presente in locale: " + fname)
                    try:
                        nc = Dataset(url1)
                        nc.close()
                        urls.append(url1)
                        # filename = wget.download(url1,"/Users/rosarioriccio/PycharmProjects/prova1/d01")
                        # print(filename)
                        # print("file salvato in locale: " + fname)
                        print("file presente online: " + fname)
                    except Exception as e:
                        print("file non presente online")
                else:
                    print("file presente in locale: " + fname)
                    ncfiles.append(localPath2)
        return urls,ncfiles

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


