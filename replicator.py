from pymongo import MongoClient, ReplaceOne

import argparse
import pymongo.errors
import time

def reclen(d):
    r = 0
    for k in d:
        r = r + len(d[k])
    return r

def replicate(args):
    source_uri = args.source
    target_uri = args.target
    track = args.track

    source_client = MongoClient(source_uri)
    target_client = MongoClient(target_uri)
    track_db = track[0:track.index(".")]
    track_coll = track[track.index(".") + 1:]

    tracker = target_client[track_db][track_coll]
    tracker_doc = tracker.find_one_and_update({}, {"$setOnInsert": {"token": None}}, upsert=True)
    token = None if tracker_doc == None else tracker_doc["token"]
    ctime = None if tracker_doc == None else tracker_doc["clusterTime"]
    
    try:
        with source_client.watch([{"$match": {"operationType": {"$in": ["insert", "update"]}}}],
                                 full_document="updateLookup",  resume_after=token) as stream:
            print("Starting!")
            start = int(time.time() * 1000)
            batch = {}
            for change in stream:
                tok = change["_id"]
                clustertime = change["clusterTime"]
                ns = change["ns"]["db"] + "." + change["ns"]["coll"]
                full_doc = change["fullDocument"]

                if ns in batch:
                    batch[ns].append(ReplaceOne({"_id": full_doc["_id"]}, full_doc, upsert=True))
                else:
                    batch[ns] = [ReplaceOne({"_id": full_doc["_id"]}, full_doc, upsert=True)]

                t = int(time.time() * 1000)
                if reclen(batch) >= 100 or (t-start) > 1000: # flush every 100 docs or 1000ms
                    for ns in batch:
                        db = ns[0:ns.index(".")]
                        coll = ns[ns.index(".")+1:]
                        target_client[db][coll].bulk_write(batch[ns], ordered=False)

                    tracker.update_one({}, {"$set": {"token": tok, "clusterTime": clustertime}}, upsert=True)
                    print("Bulk insert of %d events, batch started %dms ago" % (reclen(batch), t-start))
                    batch = {}
                    start = t

    except pymongo.errors.PyMongoError as error:
        print("error: %s" % str(error))


if __name__ == "__main__":
    parser = argparse.ArgumentParser("MongoDB Replicator - use change streams to copy data from one cluster to another")
    options = parser.add_argument_group("Mandatory options")
    options.add_argument("--source", help="Source URI", required=True)
    options.add_argument("--target", help="Target URI", required=True)
    options.add_argument("--track", help="Namespace for tracking document in target (default: replicator.tracking)", required=False, default="replicator.tracking")
 
    parsed_args = parser.parse_args()
    replicate(parsed_args)