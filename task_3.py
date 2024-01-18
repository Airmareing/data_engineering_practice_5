import pymongo
import msgpack
import json

def create_and_update_db():
    client = pymongo.MongoClient()
    db = client['task1_db']
    collection = db['workers']

    with open('./data/task_3_item.msgpack', 'rb') as file:
        data = msgpack.unpack(file, raw=False)

        for record in data:
            collection.insert_one(record)

    collection.delete_many({'$or': [{'salary': {'$lt': 25000}}, {'salary': {'$gt': 175000}}]})
    collection.update_many({}, {'$inc': {'age': 1}})
    collection.update_many({'profession': {'$in': ['Manager', 'Developer']}}, {'$mul': {'salary': 1.05}})
    collection.update_many({'city': 'New York'}, {'$mul': {'salary': 1.07}})
    collection.update_many({'$and': [{'city': 'San Francisco'}, {'profession': {'$in': ['Manager', 'Developer']}},
                                     {'age': {'$gt': 30, '$lt': 50}}]}, {'$mul': {'salary': 1.1}})
    collection.delete_many({'age': {'$lt': 25}})

    result = collection.find().sort([('age', pymongo.ASCENDING), ('salary', pymongo.DESCENDING)]).limit(15)
    with open("./3/result_aggregate_6.json", 'w', encoding='utf-8') as result_file:
        json.dump([{"_id": str(item["_id"]), "age": item["age"], "salary": item["salary"]} for item in result], result_file, ensure_ascii=False, indent=2)

    result = collection.find().sort([('age', pymongo.DESCENDING), ('salary', pymongo.ASCENDING)]).limit(15)
    with open("./3/result_aggregate_7.json", 'w', encoding='utf-8') as result_file:
        json.dump([{"_id": str(item["_id"]), "age": item["age"], "salary": item["salary"]} for item in result], result_file, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    create_and_update_db()
