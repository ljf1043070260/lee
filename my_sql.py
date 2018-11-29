import sqlite3
conn = sqlite3.connect('spider_top250.db')
c = conn.cursor()
def create_database(name="spider_top250.db"):
    conn = sqlite3.connect(name)
    c = conn.cursor()
    sql = '''CREATE TABLE IF NOT EXISTS book_top250(
    id INT PRIMARY KEY ,
    title   TEXT,
    name   TEXT, 
    publishing_company   TEXT, 
    produces    TEXT,
    original_name   TEXT, 
    translator   TEXT, 
    year   TEXT, 
    pages   TEXT, 
    money   TEXT,
    bindings   TEXT, 
    series   TEXT, 
    isbn   TEXT,    
    intro   TEXT 
    );'''
    c.execute(sql)
    conn.commit()
    print("创建数据表完成")




def insert_database(id,book_info=[["test","test","test","test","test","test","test","test","test","test","test","test"]]):
    c.execute('''INSERT INTO book_top250 (id,title,name,publishing_company,
    produces,original_name,translator,year,pages,money,bindings,series,isbn,intro) 
    VALUES 
    (?,?,?,?,?,?,?,?,?,?,?,?,?,?);''',
    (id,book_info[0][11],book_info[0][0],book_info[0][1],book_info[0][2],book_info[0][3],book_info[0][4],book_info[0][5],book_info[0][6],book_info[0][7],book_info[0][8],book_info[0][9],book_info[0][10],book_info[0][12]))
    conn.commit()
    print("id:"+str(id)+"++++++插入完成一条++++++")
    
   

def close_databse():
    conn.close()    
    print("关闭数据库连接")

# create_database()
# insert_database(1,[('[日]东野圭吾', '南海出版公司', '新经典文化', 'ナミヤ雑貨店の奇蹟', '李盈春', '2014-5', '291', '39.50元', '精装', '新经典文库·东野圭吾作品', '9787544270878', '解忧杂货店', '简介')])
# close_databse()