import shutil
import leveldb
import os
dirname = "./data2"
if os.path.exists(dirname):
    shutil.rmtree(dirname)
db = leveldb.LevelDB(dirname)
k = '1'.encode()
v = '1'.encode()
v2 = '2'.encode()
db.Put(k, v)
print(db.Get(k))
db.Put(k, v2)
print(db.Get(k))
db.Delete(k)
v = db.Get(k)
print(v)
# print(db.Get(k))