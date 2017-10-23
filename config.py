#coding:utf-8
import os
from Log import *
LISTEN_PORT        = port           
CHN_STATUS_INIT    = 1               
CHN_STATUS_OK      = 2               
CHN_STATUS_ALERT   = 3              
CHN_STATUS_STOP    = 99           
log_name="transcode.log"
log_dir="logs"

if not os.path.exists(log_dir):
    os.mkdir(log_dir)

logging = Logger(logname=os.path.join(log_dir,log_name), loglevel=1, logger="transcode_main").getlog()
