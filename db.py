#coding=utf-8
import MySQLdb

class DB_MYSQL :
    
    def __init__(self):
        self.conn = None
        self.cur = None
        
            
    def connect(self, host, port, user, passwd, db, charset='utf8') :
        self.conn = MySQLdb.connect(host, user, passwd, db, port, charset='utf8')
        self.cur  = self.conn.cursor()
        return True
        
    def execute(self, sql):           
        return self.cur.execute(sql)

    def commit(self):           
        return self.conn.commit();
        
    def close(self):
        self.cur.close()
        self.conn.close()
  
class DB_CONFIG:

    host        = ip
    port        = port
    user        = 'user'
    password    = 'password'
    db          = 'db_name'
    
        
