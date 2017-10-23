import os
import requests
import subprocess
import sh
from celery.task import task
from celery import Celery
from celery.task import chord

celery = Celery('tasks', backend='redis://*.*.*.*:****/*', broker='redis://*.*.*.*:****/*')
@celery.task(bind = True, default_retry_delay = 30 * 60)
def cut(self, name):
    try:
        ffmpeg = sh.Command('/usr/local/bin/ffmpeg');
        ffmpeg('-y', '-i', name, '-threads', '1',
            '-acodec', 'copy', '-f', 'segment', '-segment_time', '360',
            '-vcodec', 'copy', '-reset_timestamps', '1', '-map', '0', '-an', '%d.ts');

        return 1;

    except Exception as exc:
        raise self.retry(exc = exc, countdown = 10);

@celery.task(bind = True, default_retry_delay = 30 * 60)
def transcode(self, path, url,param_project_name,save_path,ffmpeg_transcode):
    try:
        name = url.split('/')[-1].split('?')[0]; 
        r = requests.get(url, allow_redirects = True, stream = True, timeout = (4, 128));
        if r.status_code == 200:
            with open(path + name, 'w+') as fd:
                for chunk in r.iter_content(1024):
                    fd.write(chunk);
        t_shell = '/usr/local/bin/'+ffmpeg_transcode
        print t_shell
        subprocess.call(t_shell, shell=True)
        return path + '_' + name;
    except Exception as exc:
        raise self.retry(exc = exc, countdown = 10);


@celery.task(bind = True, default_retry_delay = 30 * 60)
def cocat(self, names,param_project_name,save_path,ffmpeg_cocat):
    try:
        path = names[0].split('/')[4];
        txt = save_path+path + '.txt';
        f = open(txt, 'w+');
        for name in names:
            f.write("file " + "'" + name.split('/')[3]+'/'+name.split('/')[4]+'/'+name.split('/')[5] + "'\n");
        f.close();

        path += '.ts';
        t_shell = '/usr/local/bin/'+ffmpeg_cocat
        print t_shell
        subprocess.call(t_shell, shell=True)
        return 1;

    except Exception as exc:
        raise self.retry(exc = exc, countdown = 10);
