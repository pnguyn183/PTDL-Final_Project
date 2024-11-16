import scrapy
import pymongo
import json
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import csv
import os

class MongoDBCarPipeline:
    def __init__(self):
        # Connection String
        econnect = str(os.environ.get('Mongo_HOST', 'localhost'))
        # self.client = pymongo.MongoClient('mongodb://mymongodb:27017')
        self.client = pymongo.MongoClient('mongodb://'+econnect+':27017')
        self.db = self.client['dbcardata'] #Create Database      
        pass

    def process_item(self, item, spider):

        collection =self.db['cars'] #Create Collection or Table
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")       
        pass

class CSVDBCarPipeline:
    def __init__(self):
        self.filepath = 'csvdatacar.csv'
        # Kiểm tra xem file đã tồn tại chưa, nếu chưa thì thêm dòng tiêu đề
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file, delimiter=',')
                writer.writerow([
                    'Name', 'Price', 'Year', 'Style', 'Made In', 'Kilometer', 
                    'Province', 'District', 'Gearbox', 'Fuel', 'Description'
                ])

    def process_item(self, item, spider):
        with open(self.filepath, 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow([
                item['car_name'], 
                item['car_price'],
                item['year'], 
                item['style'], 
                item['madein'], 
                item['kilometer'],
                item['province'],
                item['district'], 
                item['gearbox'],
                item['fuel'],
                item['descri']
            ])
        return item

class JsonDBCarPipeline:
    def open_spider(self, spider):
        self.file = open('jsondatacar.json', 'w', encoding='utf-8')
        self.file.write('[')
        self.first_item = True  # Biến kiểm tra mục đầu tiên

    def close_spider(self, spider):
        self.file.write(']')
        self.file.close()

    def process_item(self, item, spider):
        if not self.first_item:
            self.file.write(',\n')  # Thêm dấu phẩy trước các mục sau
        else:
            self.first_item = False
        line = json.dumps(dict(item), ensure_ascii=False)
        self.file.write(line)
        return item