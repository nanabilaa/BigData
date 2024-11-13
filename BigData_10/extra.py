from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['Market_db']
collection = db['Products']
print("connected to MonngoDb")

collection.delete_many({}) #menghindari duplikasi produk
print('no duplicate')

products = [
    {'name' : 'Indomie', 'category' : 'instant food', 'price' : 4000, 'rack' : 'A'},
    {'name' : 'Fruit Loops', 'category' : 'cereal', 'price' : 30000, 'rack' : 'B'},
    {'name' : 'Samyang Buldak Carbonara', 'category' : 'instant food', 'price' : 35000, 'rack' : 'A'},
    {'name' : 'Chicken Dimsum', 'category' : 'frozen food', 'price' : 45000, 'rack' : 'C'},
    {'name' : 'Chicken Podridge', 'category' : 'instant food', 'price' : 15000, 'rack' : 'A'},
    {'name' : 'Bihun', 'category' : 'instant food', 'price' : 2000, 'rack' : 'A'},
    {'name' : 'Chicken Wings', 'category' : 'frozen food', 'price' : 48000, 'rack' : 'C'},
    {'name' : 'Topokki', 'category' : 'instant food', 'price' : 25000, 'rack' : 'A'},
    {'name' : 'French Fries', 'category' : 'frozen food', 'price' : 40000, 'rack' : 'C'},
    {'name' : 'Coco Chrunch', 'category' : 'cereal', 'price' : 31000, 'rack' : 'B'},
    {'name' : 'Corn Flakes', 'category' : 'cereal', 'price' : 28000, 'rack' : 'B'},
    {'name' : 'Honey Stars', 'category' : 'cereal', 'price' : 32000, 'rack' : 'B'},
    {'name' : 'Chips Ahoy', 'category' : 'cereal', 'price' : 38000, 'rack' : 'B'},
    {'name' : 'Milo Balls', 'category' : 'cereal', 'price' : 35200, 'rack' : 'B'},
    {'name' : 'Vanila Milk', 'category' : 'milk', 'price' : 2200, 'rack' : 'B'},
]
collection.insert_many(products) #INSERT
print("Data Inserted")


print('----DETAIL PRODUCTS------------------------')
#MENAMPILKAN PRODUCTS
for product in collection.find():
    print(product)
print('------------------------------------------')

#PRODUK DIATAS AVARAGE
average = collection.aggregate([
    {'$group': {'_id': None, 'average': {'$avg': '$price'}}}])
average = list(average)[0]['average'] if average else 0
diatas_average = collection.find({'price' : {'$gt':average}})
print("PRODUK DENGAN HARGA DIATAS AVERAGE: ")
for product in diatas_average:
    print(product)
print('DENGAN AVERAGE PRODUK ADALAH:', average)

#TOTAL PRODUK DALAM SETIAP KATEGORI
kategori = collection.aggregate([
    {'$group' : {"_id" : '$category', 'total_produk': {'$sum': 1}}}
])
print('TOTAL PRODUK PADA KETEGORI: ')
for category in kategori:
    print(category)

