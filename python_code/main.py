import time,json,pymongo
from google.cloud import pubsub_v1

# TODO(developer)
project_id = "vigilant-yeti-343907"
topic_id = "mongo-bq-test"

publisher = pubsub_v1.PublisherClient.from_service_account_json("./../vigilant-yeti-343907-13ab28347e73.json")
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id="vigilant-yeti-343907",
    topic='mongo-bq-test',
)

client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.lsbxu.mongodb.net/sample_mflix")
db = client['sample_mflix']['comments']
collection = db['comments']

try:
    with db.watch([{'$match': {'operationType': 'insert'}}]) as stream:
        for insert_change in stream:
            new_doc = insert_change['fullDocument']
            new_doc.pop('_id',None)
            print(new_doc)
            future = publisher.publish(topic_name, json.dumps(new_doc, indent=2).encode('utf-8'))
            future.result()

except pymongo.errors.PyMongoError:
    # The ChangeStream encountered an unrecoverable error or the
    # resume attempt failed to recreate the cursor.
    print("something went wrong!")