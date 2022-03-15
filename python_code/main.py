import time,json,pymongo
from google.cloud import pubsub_v1
from datetime import datetime

publisher = pubsub_v1.PublisherClient.from_service_account_json("./../vigilant-yeti-343907-13ab28347e73.json")
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id="vigilant-yeti-343907",
    topic='mongo-bq-test-2',
)

client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.lsbxu.mongodb.net/sample")
db = client['sample']['comments']

try:
    with db.watch() as stream:
        for insert_change in stream:
            temp = {}
            if(insert_change['operationType'] == 'insert'):
                temp.update(insert_change['fullDocument'])
                temp.pop('_id',None)
                temp['id'] = str(insert_change['documentKey']['_id'])
                temp['timestamp'] = str(datetime.now())
                temp['operation'] = insert_change['operationType']
            elif insert_change['operationType'] == 'update':
                temp = insert_change['updateDescription']['updatedFields']
                temp['id'] = str(insert_change['documentKey']['_id'])
                temp['timestamp'] = str(datetime.now())
                temp['operation'] = insert_change['operationType']

            print(temp)
            future = publisher.publish(topic_name, json.dumps(temp, indent=2).encode('utf-8'))
            future.result()

except pymongo.errors.PyMongoError as e:
    # The ChangeStream encountered an unrecoverable error or the
    # resume attempt failed to recreate the cursor.
    print(e)
    print("something went wrong!")