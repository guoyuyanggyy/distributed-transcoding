#coding: utf-8
import sys
import string
import logging
import json
import db
import config
import datetime

import os
import requests
import redis
import time
from celery.task import chord
from tasks import cocat, transcode, cut
from celery import chain
from celery import group
from multiprocessing import Process
task_list = {'task_jiangsu':False,'task_sichuan':False,'task_hebei':False,'task_shandong':False}
def GetffmpegTrans(path,line,param_project_name):
    try:
        name = line.split('/')[-1].split('?')[0];
        if param_project_name=='task_sichuan':  #u'四川'
            ffmpeg_transcode = ('ffmpeg -y -i %s%s -threads 1 -aspect 16:9 -refs 1 -profile:v high -level 41 -vb 8000k -vf "scale=iw*min(1920/iw\,720/ih):ih*min(1920/iw\,720/ih), pad=1920:720:(1920-iw*min(1920/iw\,720/ih))/2:(720-ih*min(1920/iw\,720/ih))/2" -vcodec libx264 -x264opts vbv-bufsize=8000:vbv-maxrate=8000:force-cfr:nal-hrd=cbr:b-pyramid=0 -acodec copy %s_%s') % (path,name,path,name)
        elif param_project_name=='task_jiangsu':#u'江苏'
            ffmpeg_transcode = ''
        elif param_project_name=='task_hebei':  #u'河北'
            ffmpeg_transcode = ''
        elif param_project_name=='task_shandong':#u'山东4K'
            ffmpeg_transcode = ('ffmpeg -y -i %s%s -threads 1 -s 3840x1600 -aspect 16:9 -vb 10M -maxrate 10M -minrate 8M -copyts -vcodec libx265 -acodec copy -y %s_%s') % (path,name,path,name)
        elif param_project_name=='task_xinjiang':#u'新疆'
            ffmpeg_transcode = ''
        else:
            ffmpeg_transcode = ''
    except Exception,e:
        config.logging.error("GetffmpegTrans.error=[%s]\n" % (e))
        return False
    #config.logging.info('param_project_name=[%s],ffmpeg_transcode=[%s]'%(param_project_name,ffmpeg_transcode))
    return ffmpeg_transcode

def GetffmpegCocat(names,param_project_name,save_path):
    try:
        path = names[0].split('/')[4];
        txt = save_path+path + '.txt';
        path += '.ts';
        if param_project_name=='task_sichuan': #u'四川'
            ffmpeg_cocat = 'ffmpeg -threads 20 -y -i "concat:'
            for i in range(0,len(names)):
                if i==(len(names)-1):
                    ffmpeg_cocat = ffmpeg_cocat+names[i]
                else:
                    ffmpeg_cocat = ffmpeg_cocat+names[i]+'|'
            ffmpegend = '" -sdt_period 0.5 -compute_pcr 0 -vcodec copy -acodec copy -metadata service_provider="GITV" -metadata service_name="1.1.0Tong" %s'%(save_path+path)
            ffmpeg_cocat +=ffmpegend;
        elif param_project_name=='task_jiangsu':#u'江苏'
            ffmpeg_cocat = 'ffmpeg -threads 20 -y -i "concat:'
            for i in range(0,len(names)):
                if i==(len(names)-1):
                    ffmpeg_cocat = ffmpeg_cocat+names[i]
                else:
                    ffmpeg_cocat = ffmpeg_cocat+names[i]+'|'
            ffmpegend = '" -r 25 -profile:v high -level 41 -vcodec libx264 -vb 7300k -vf "scale=iw*min(1920/iw\,720/ih):ih*min(1920/iw\,720/ih), pad=1920:720:(1920-iw*min(1920/iw\,720/ih))/2:(720-ih*min(1920/iw\,720/ih))/2" -x264opts vbv-bufsize=7300:vbv-maxrate=7300:keyint=25:force-cfr:nal-hrd=cbr -sdt_period 0.5 -compute_pcr 0 -muxrate 8000k -ab 128k -ar 48000 -acodec mp2 -metadata service_provider="GITV" -metadata service_name="1.1.0Tong" %s'%(save_path+path)
            ffmpeg_cocat +=ffmpegend;
        elif param_project_name=='task_hebei':#u'河北'
            ffmpeg_cocat = 'ffmpeg -threads 20 -y -i "concat:'
            for i in range(0,len(names)):
                if i==(len(names)-1):
                    ffmpeg_cocat = ffmpeg_cocat+names[i]
                else:
                    ffmpeg_cocat = ffmpeg_cocat+names[i]+'|'
            ffmpegend = '" -r 25 -vb 4000k -vcodec libx264 -x264opts vbv-bufsize=4000:vbv-maxrate=4000:keyint=25:force-cfr:nal-hrd=cbr -sdt_period 0.5 -compute_pcr 0 -ab 64k -ar 48000 -acodec libfaac -metadata service_provider="GITV" -metadata service_name="1.1.0Tong" %s'%(save_path+path)
            ffmpeg_cocat +=ffmpegend;
        elif param_project_name=='task_shandong':#u'山东4K'
            ffmpeg_cocat = ''
        elif param_project_name=='task_xinjiang':#u'新疆'
            ffmpeg_cocat = ''
        else:
            ffmpeg_cocat = ''
    except Exception,e:
        config.logging.error("GetffmpegCocat.error=[%s]\n" % (e))
        return False
    config.logging.info('param_project_name=[%s],ffmpeg_cocat=[%s]'%(param_project_name,ffmpeg_cocat))
    return ffmpeg_cocat

def UpdateStatus(State,param_project_name,m3u8):
    try: 
        self_db = db.DB_MYSQL();
        self_db.connect(db.DB_CONFIG.host, db.DB_CONFIG.port, db.DB_CONFIG.user, db.DB_CONFIG.password, db.DB_CONFIG.db)
    except Exception, e:
        config.logging.error('UpdateStatus().connect to db, error=[%s]' %(e))
    onesql='';
    try:
        curtime = datetime.datetime.fromtimestamp(float(time.time())).strftime('%Y-%m-%d %H:%M:%S');
        onesql = "UPDATE %s SET task_status=%s,transtime='%s' WHERE m3u8='%s'" %(param_project_name,State,curtime,m3u8)
        self_db.execute(onesql);
        self_db.commit();
        self_db.close();
    except Exception, e:
        config.logging.error('UpdateStatus(), error=[%s],sql=[%s]' %(e,onesql))

def Update_taskItems_Status(param_status,param_project_name):
    try: 
        self_db = db.DB_MYSQL();
        self_db.connect(db.DB_CONFIG.host, db.DB_CONFIG.port, db.DB_CONFIG.user, db.DB_CONFIG.password, db.DB_CONFIG.db)
    except Exception, e:
        config.logging.error('UpdateStatus().connect to db, error=[%s]' %(e))
    onesql='';
    try:
        curtime = datetime.datetime.fromtimestamp(float(time.time())).strftime('%Y-%m-%d %H:%M:%S');
        onesql = "UPDATE task_items SET Status=%s,End_time='%s' WHERE Task_name='%s'" %(param_status,curtime,param_project_name)
        self_db.execute(onesql);
        self_db.commit();
        self_db.close();
    except Exception, e:
        config.logging.error('UpdateStatus(), error=[%s],sql=[%s]' %(e,onesql))


def jobs(param_project_name):
    "ffmpeg -i fff.avi -acodec copy -f segment -segment_time 10 -vcodec copy -reset_timestamps 1 -map 0 -an fff%d.avi"
    info_list=list();
    retry_num=0;
    while 1:
        try: 
            self_db = db.DB_MYSQL();
            self_db.connect(db.DB_CONFIG.host, db.DB_CONFIG.port, db.DB_CONFIG.user, db.DB_CONFIG.password, db.DB_CONFIG.db)
        except Exception, e:
            config.logging.error('jobs.connect to db_task_name, error=[%s]' %(e))
        onesql='select job_name,m3u8 from %s where task_status=0 or task_status=99  ORDER BY createtime  LIMIT 1'%(param_project_name);
        print onesql 
        try:
            result = self_db.execute(onesql)
            if result<=0:
                config.logging.info('process.jobs end,video_list=[%s],param_project_name=[%s]' %(info_list,param_project_name))
                Update_taskItems_Status(0,param_project_name);  #u'项目本次转码结束'
                break;
            for row in self_db.cur.fetchall():
                info_list = []
                info_list.append(row)
            self_db.close()
        except Exception,e:
            config.logging.error('EpgToolDb.Qurey().jobs, error=[%s],one_sql_records=[%s]' %(e,onesql))
            time.sleep(300)
            Update_taskItems_Status(99,param_project_name);  #u'连接数据库超时'
            continue
        
        urls = info_list
        "select * from task_sichuan where task_status=0 ORDER BY createtime  LIMIT 1"#按时间顺序查最近一条
        if len(urls) == 0:
            config.logging.info('process.jobs end,video_list=[%s],param_project_name=[%s]' %(info_list,param_project_name))
            Update_taskItems_Status(0,param_project_name);
            break;
        try:
            url = urls[0][1];                      #m3u8
            config.logging.info("urls[0][1]=[%s]" % (url))
            bid = url.split('/')[-1].split('.')[0];
            vid = url.split('/')[-2];
            aid = url.split('/')[-3];
        except Exception,e:
            #print e,urls           #解析错误，置为-1，跳过本条
            UpdateStatus(-1,param_project_name,url);
            continue;
        try:
            os.mkdirs('./'+param_project_name+'/'+info_list[0][0]+'/'+"tmp/%s_%s_%s/" % (aid, vid, bid));
        except:
            pass;
        path = './'+param_project_name+'/'+info_list[0][0]+'/'+"tmp/%s_%s_%s/" % (aid, vid, bid);#u'创建分片转码前后文件存放目录，下划线为转码后的ts片'
        s = list();

        req = None;
        retry = 0;
        while 1:
            try:
                #print url;
                req = requests.get(url, timeout = (4, 4));
            except requests.exceptions.RequestException as e:
                retry += 1;
                if retry < 3:
                    time.sleep(3);
                    continue;
            break;

        if not req:
            #info_list.pop(0)
            config.logging.error("request timeout m3u8 =[%s]\n" % (url))
            UpdateStatus(-1,param_project_name,url);
            continue;

        lines = list();
        for line in req.text.split('\n'):
            if line.find('.ts') != -1:
                lines.append(line);

        try:
            req = requests.head(lines[-1], allow_redirects = True, timeout = (4, 4));
            if int(req.headers['Content-Length']) < 2000:
                lines.pop();
        except requests.exceptions.RequestException as e:
            pass;
        try:
            names=list();
            ffmpeg_transcode=''
            ffmpeg_cocat=''
            #更新为转码中状态
            UpdateStatus(99,param_project_name,info_list[0][1]);
            for line in lines:
                ffmpeg_transcode = GetffmpegTrans(path,line,param_project_name);#获取转码命令
                #print ffmpeg_transcode
                s.append(transcode.subtask((path, line,param_project_name,save_path,ffmpeg_transcode)));
                if param_project_name=='task_jiangsu' or param_project_name=='task_hebei':
                    names.append(path + line.split('/')[-1].split('?')[0]);
                else:
                    names.append(path + '_' + line.split('/')[-1].split('?')[0]);
            config.logging.info('param_project_name=[%s],ffmpeg_transcode=[%s]'%(param_project_name,ffmpeg_transcode));
            try:
                chain(group(s))().get();#转码完成
                cocatlist=list();
                ffmpeg_cocat = GetffmpegCocat(names,param_project_name,save_path);#获取合并命令
                if ffmpeg_cocat != '':
                    cocatlist.append(cocat.subtask((names,param_project_name,save_path,ffmpeg_cocat)));
                chain(group(cocatlist))().get();#合并完成
                #更新为转码完成状态
                UpdateStatus(1,param_project_name,info_list[0][1]);
            except Exception,e:
                retry_num += 1;
                if retry_num < 3:
                    time.sleep(60);
                    continue; 
                #更新为转码失败状态
                UpdateStatus(-1,param_project_name,info_list[0][1]);
                config.logging.error("cocat.error=[%s]\n" % (e))
            info_list.pop(0)
            retry_num = 0
            config.logging.info("info_list.pop(0)=[%s]\n" % (url))
        except Exception,e:
            retry_num += 1;
            if retry_num < 3:
                time.sleep(60);
                continue
            info_list.pop(0)
            retry_num = 0
            UpdateStatus(-1,param_project_name,url);
            config.logging.error('jobs.transcode or cocat error,e=[%s],ts_urls=[%s]' %(e,lines))
            config.logging.error("ffmpeg_transcode=[%s], ffmpeg_cocat =[%s]\n" % (ffmpeg_transcode,ffmpeg_cocat))


class TranscodeToolDb:
    def __init__(self):
        self.the_db = db.DB_MYSQL()
        try: 
            self.the_db.connect(db.DB_CONFIG.host, db.DB_CONFIG.port, db.DB_CONFIG.user, db.DB_CONFIG.password, db.DB_CONFIG.db)
        except Exception, e:
            config.logging.error('EpgToolDb.Init(), error=[%s]' %(e))

    def QueryAll(self,param_project_name):
        one_sql_records = "select * from task_items where Task_name='%s' " % (param_project_name)
        result =-1
        try:
            result = self.the_db.execute(one_sql_records)
        except Exception,e:
            config.logging.error('EpgToolDb.Qurey(), error=[%s],one_sql_records=[%s]' %(e,one_sql_records))
            return result;
        if result<=0:
            return result;
        task_list = list()
        for row in self.the_db.cur.fetchall():
            task_list.append(row)   
        #启进程

        try:
            if task_list[0][2]==0:
                process = Process(target=jobs,args=(param_project_name,))
                process.start()
                curtime = datetime.datetime.fromtimestamp(float(time.time())).strftime('%Y-%m-%d %H:%M:%S');
                one_sql_lockStatus = "UPDATE task_items SET Status=1,Start_time='%s' WHERE Task_name='%s'"%(curtime,param_project_name)
                self.the_db.execute(one_sql_lockStatus)
                self.the_db.commit()
                config.logging.info('process start OK ,task_list=[%s],param_project_name=[%s]' %(task_list,param_project_name))
            else:
                config.logging.info('process is existing ,task_list=[%s],param_project_name=[%s]' %(task_list,param_project_name))
        except Exception,e:
            result=-1
            config.logging.error('Process.error ,e=[%s],task_list=[%s],param_project_name=[%s]' %(e,task_list,param_project_name))
        try:
            sql_records = "select count(*) from %s where task_status=0 " % (param_project_name)
            result = self.the_db.execute(sql_records)
            for row in self.the_db.cur.fetchall():
                result = row[0]
            config.logging.info('EpgToolDb.Qurey(), param_project_name=[%s],the task count=[%s],sql_records=[%s]' %(param_project_name,result,sql_records))
        except Exception,e:
            config.logging.error('EpgToolDb.Qurey(), param_project_name=[%s],error=[%s],sql_records=[%s]' %(param_project_name,e,sql_records))
            return -1;
        self.the_db.close();
        return (result)
