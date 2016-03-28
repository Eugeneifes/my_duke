# -*- coding: utf-8 -*-
from pskov.rest.handlers.secure_handler import SecureHandler
from tornado.web import asynchronous, authenticated, HTTPError
from tornado.gen import coroutine
from pskov.utils.connection_manager import ConnectionManager
import json


class DukeHandler(SecureHandler):
    url = r"/duke/(.*)"

    @asynchronous
    @coroutine
    def get_merge_collection(self):
        from bson import ObjectId
        report_id = self.get_argument("id")
        report = self.mongo.duplicates.find_one({"_id": ObjectId(report_id)})
        self.write_json(self.transform(report))

    @staticmethod
    def transform(doc):
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        if "datetime" in doc:
            doc["datetime"] = doc["datetime"].isoformat()
        return doc

    @asynchronous
    @coroutine
    def sorted_by_date_collections(self):

        skip_c = self.get_argument("skip", 0)
        count = self.get_argument("count", 10)
        sample = ConnectionManager.mongodb.duplicates.find({})
        if skip_c:
            sample.skip(skip_c)
        if count:
            sample.limit(count)
        d = [self.transform(v) for v in sample.sort("datetime", 1)]
        self.write(json.dumps(d))


    @authenticated
    def get(self, _type):
        types = {
        "get_merge_collection": self.get_merge_collection,
        "sorted_by_date_collections": self.sorted_by_date_collections
        }
        if types.get(_type):
            types.get(_type)()
        else:
            raise HTTPError(404)








