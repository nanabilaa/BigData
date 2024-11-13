from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['company_db']  
collection = db['employees']  

#memastikan koneksi
print('Connected to MongoDB')

#Menghindari duplikasi data
collection.delete_many({})
print("No Data Duplikasi")


2. #OPERASI CRUD DASAR
Data = {
    'name' : 'Alice',
    'dept' : 'Finance', 
    'age' : 29,
    'salary' : 4500,
    'city' : 'New York'
}, {
    'name' : 'Jason',
    'dept' : 'Market',
    'age' : 25,
    'salary' : 2000,
    'city' : 'California'
},{
    'name': 'Michelle',
    'dept' : 'HR',
    'age' : 28,
    'salary' : 4800,
    'city' : 'Arizona'
},{
    'name' : 'Jackson',
    'dept' : 'Finance',
    'age' : 24,
    'salary' : 2900,
    'city' : 'Colorado'
},{
    'name' : 'Karen',
    'dept' : 'HR',
    'age' : 27,
    'salary' : 2500,
    'city' : 'Nevada'
},{
    'name' : 'Olivia',
    'dept' : 'Finance',
    'age' : 25,
    'salary' : 3000,
    'city' : 'Indiana'
},{
    'name' : 'Dowwy',
    'dept' : 'Market',
    'age' : 29,
    'salary' : 3200,
    'city' : 'Pensylvania'  
},{
    'name' : 'Louis',
    'dept' : 'Market',
    'age' : 26,
    'salary' : 2700,
    'city' : 'Alabama'
}




collection.insert_many(Data) #INSERT 
print('Data employee inserted')

for Data in collection.find():
    print(Data)

#UPDATE
collection.update_one(
    {'name' : 'Jason'},
    {'$set' : {'age' : 24}}
)
collection.update_one(
    {'name' : 'Alice'},
    {'$set' : {'city' : 'California'}}
)
print('Data Updated')

for employee in collection.find():
    print(employee)


#3 Query Aggregation untuk mencari rata-rata gaji per departemen
pipeline = [
    {'$group': {'_id': '$department', 'average_salary': {'$avg': '$salary'}}}
]
for result in collection.aggregate(pipeline):
    print(result)

#GAJI TERTINGGI SETIAO DEPT
pipeline = [
    {'$sort': {'dept': 1, 'salary': -1}},  # Mengurutkan berdasarkan departemen (asc) dan gaji (desc)
    {'$group': {
        '_id': '$dept',  
        'pekerja_tertinggi': {'$push': {'name': '$name', 'salary': '$salary'}}
    }},
    {'$project': {
        'pekerja_tertinggi': {'$slice': ['$pekerja_tertinggi', 5]} 
    }}
]
#HASIL
for salary in collection.aggregate(pipeline):
    print("Departemen:", salary['_id'])
    for employees in salary['pekerja_tertinggi']:
        print(employees)

#DATA UPDATE; MENGAHPUS KARYAWAN UNDER 25
collection.delete_many({'age': {'$lt': 25}})
print("Karyawan dengan usia di bawah 25 tahun telah dihapus.")

for data in collection.find():
    print(data)

#LAPORAN
pipeline = [
    {'$group': {
        '_id': '$dept',  # kelompokkan berdasarkan dept
        'total_salary': {'$sum': '$salary'},  # total gaji tiap dept
        'average_age': {'$avg': '$age'}  # avg umur tiap dept
    }}
]

# HASIL
for result in collection.aggregate(pipeline):
    print("Departement:", result['_id'])
    print("Total Salary:", result['total_salary'])
    print("Average Of Age:", result['average_age'])
    print()






