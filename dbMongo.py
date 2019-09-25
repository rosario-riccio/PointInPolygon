"""This file contains ManageDB class to manage DB"""

from pymongo import MongoClient

class ManageDB(object):
    def __init__(self):
        """This is constructor"""
        try:
            self.client = MongoClient("mongodb://localhost:27017/")
            db = self.client.MediStormSeekerDB
        except Exception as e:
            print("db not ready " + str(e))
        self.db = db

    def getPolygonOnDate(self,date1):
        """This method gets polygons of specific date"""
        count = self.db.PolygonCollection.find({"properties.dateStr":date1}).count()
        print("Number of polygon of the date ",date1," n.",count)
        cursorPolygon = self.db.PolygonCollection.find({"properties.dateStr":date1})
        return cursorPolygon

    def insertPolygonDB(self,polygonGeoJson):
        """This method insert a polygon in DB"""
        id= self.db.PolygonCollection.insert(polygonGeoJson)
        return id

    def listLabelCollectionDB(self):
        """This method gets labels' list"""
        count = self.db.LabelCollection.find().count()
        print("number of labels:",count)
        if count > 0:
            result = True
            cursorLabelCollection = self.db.LabelCollection.find()
            return result, cursorLabelCollection
        else:
            result = False
            cursorLabelCollection = None
            return result, cursorLabelCollection

    def groupByClassPolygonDB(self):
        """This method returns polygons grouped by label from DB"""
        count = self.db.PolygonCollection.find().count()
        print("number of polygon n.",count)
        if count > 0:
            result = True
            cursorClassPolygon = self.db.PolygonCollection.aggregate(
                [{"$group": {"_id":{"name" : "$properties.name"},"count": {"$sum": 1}}}])
            return result,cursorClassPolygon
        else:
            result = False
            cursorClassPolygon = None
            return result, cursorClassPolygon

    def getPolygonOnName(self,name):
        """This method gets polygons with a specific label"""
        count = self.db.PolygonCollection.find({"properties.name": name}).count()
        print("Number of polygon without its label n.",count)
        cursorPolygon = self.db.PolygonCollection.find({"properties.name": name})
        return cursorPolygon

