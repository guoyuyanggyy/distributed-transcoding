# -*- coding: utf-8 -*- 
#
#!/usr/bin/python 
import logging
import logging.handlers
#开发一个日志系统， 既要把日志输出到控制台， 还要写入日志文件   
#logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
#logging.debug('This message should appear on the console')
import inspect
format_dict = {
    1 : logging.Formatter('[%(asctime)s][%(filename)s][%(name)s][%(levelname)s][threadid:%(thread)s][line:%(lineno)d]%(message)s'),
    2 : logging.Formatter('[%(asctime)s][%(filename)s][%(name)s][%(levelname)s][threadid:%(thread)s][line:%(lineno)d]%(message)s'),
    3 : logging.Formatter('[%(asctime)s][%(filename)s][%(name)s][%(levelname)s][threadid:%(thread)s][line:%(lineno)d]%(message)s'),
    4 : logging.Formatter('[%(asctime)s][%(filename)s][%(name)s][%(levelname)s][threadid:%(thread)s][line:%(lineno)d]%(message)s'),
    5 : logging.Formatter('[%(asctime)s][%(filename)s][%(name)s][%(levelname)s][threadid:%(thread)s][line:%(lineno)d]%(message)s')
}

class Logger():
    def __init__(self, logname, loglevel, logger):
        '''
           指定保存日志的文件路径，日志级别，以及调用文件
           将日志存入到指定的文件中
        '''

        # 创建一个logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)

        # 创建一个handler，用于写入日志文件
        fh=logging.handlers.TimedRotatingFileHandler(logname, when='H', interval=12, backupCount=180)
        fh.setLevel(logging.DEBUG)

        # 定义handler的输出格式
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = format_dict[int(loglevel)]
        fh.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(fh)

    def getlog(self):
        return self.logger
