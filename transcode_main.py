# coding: utf-8
import sys
import os
import time
import json
import logging
import tornado
import tornado.ioloop
import tornado.web
import tornado.httpserver
import config
from Log import *
import db
import redis
import transcode_db
from multiprocessing import Process
class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("album.html")
class TransCodeHandler(tornado.web.RequestHandler):
    def get(self):
       
        self.handle()
        
    def post(self):
        self.handle()
        
    def handle(self):
        param_project_name      = self.get_argument("project_name")
        config.logging.info('TransCodeHandler.db_table_name project_name=[%s]'%(param_project_name))
        project_mo_db = transcode_db.TranscodeToolDb();
        result = project_mo_db.QueryAll(param_project_name);
        if result>=1:
            result="{'code':'转码任务下发成功,当前排队片单:%s个','MSG':%s}"%(result,result);
        elif result==0:
            result="{'code':'当前没有片单数为0,请检查后再下片单','MSG':%s}"%(result);
        elif result==-2:
            result="{'code':'当前为新建项目,请联系管理员添加权限','MSG':'未知'}";
        else:
            result="{'code':'转码失败,请联系确认参数!','MSG':%s}"%(result); 
        config.logging.info('TransCodeHandler result=[%s]'%(result)) 
        return self.write(result)
       
settings = dict(
  template_path=os.path.join(os.path.dirname(__file__), "template"),
  static_path=os.path.join(os.path.dirname(__file__), "static"),
  Debug=True,
  )
application = tornado.web.Application([
    (r"/",                        MainHandler), 
    (r"/TransCode.do",            TransCodeHandler)
    
    ],
    **settings)

def test(num):
    while 1:
        config.logging.info('heartbeat test message every 1 hour !')
        the_db = db.DB_MYSQL()
        try:
            the_db.connect(db.DB_CONFIG.host, db.DB_CONFIG.port, db.DB_CONFIG.user, db.DB_CONFIG.password, db.DB_CONFIG.db)
        except Exception, e:
            config.logging.error('test connect to db, error=[%s]' %(e))
        one_sql_records = "select count(*) from task_items  where Status =1 " 
        result = 1
        try:
            result = the_db.execute(one_sql_records)
            for row in the_db.cur.fetchall():
                result = row[0]
        except Exception,e:
            config.logging.error('test() query, error=[%s],one_sql_records=[%s]' %(e,one_sql_records))
            time.sleep(300);
            continue
        if result<=0:
            r = redis.Redis(host = '*.*.*.*', port = ****, db = *);
            r.flushdb();
        the_db.close()
        time.sleep(3600)

if __name__ == "__main__":
    reload(sys)  
    sys.setdefaultencoding('utf8')
    #test()
    process = Process(target=test,args=(10,))
    process.start()
    print 'ok'
    config.logging.info('listen @%d ......' % (config.LISTEN_PORT))
    http_server = tornado.httpserver.HTTPServer(application);
    http_server.bind(config.LISTEN_PORT);
    http_server.start(6);
    tornado.ioloop.IOLoop.instance().start();
