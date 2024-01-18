from pprint import pprint
import pymongo
import json

def create_and_upd_db():
    objects = []
    with open('./data/task_1_item.json', 'r', encoding='utf-8') as openfile:
        data = json.load(openfile)

    for item in data:
        record = {}
        record['id'] = item['id']
        record['age'] = int(item['age'])
        record['city'] = item['city']
        record['job'] = item['job']
        record['salary'] = int(item['salary'])
        record['year'] = int(item['year'])
        objects.append(record)

    client = pymongo.MongoClient()
    db = client['task1_db']
    db.drop_collection('workers')
    collection = db['workers']

    for record in objects:
        collection.insert_one(record)

    result = collection.find().sort('salary', pymongo.DESCENDING).limit(10)
    write_data_to_json(result, './1/sort_salary.json')

    result = collection.find({'age': {'$lt': 30}}).sort('salary', pymongo.DESCENDING).limit(15)
    write_data_to_json(result, './1/filter_age.json')

    result = (
        collection.find(
            {'$and': [{'city': 'Сория'}, {'job': {'$in': ['Менеджер', 'Архитектор', 'Учитель']}}]}).sort(
            'age', pymongo.ASCENDING).limit(10))
    write_data_to_json(result, './1/filter_city.json')

    result = collection.count_documents({'$and': [{'age': {'$gte': 25, '$lte': 35}},
                                                  {'year': {'$in': [2019, 2020, 2021, 2022]}}, {
                                                      '$or': [{'salary': {'$gt': 50000, '$lte': 75000}},
                                                              {'salary': {'$gt': 125000, '$lt': 150000}}]}]})
    write_data_to_json([result], './1/filter_complex.json')

def write_data_to_json(data, filename):
    items = []
    if isinstance(data, pymongo.cursor.Cursor):
        for item in data:
            item['_id'] = str(item['_id'])
            items.append(item)
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError("Unsupported data type for writing to JSON")

    with open(filename, 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    create_and_upd_db()
