# distributed-transcoding
   此系统致力于老旧的服务器的资源再利用，在CPU和磁盘还能用的情况下，
将少量的计算能力用于视频转码。
   考虑到服务器的性能有限，所以需要先将大视频文件切片，从而对小片视
频文件进行转码，每片转完码以后，在利用gfs的文件系统特性，将所有分片
合并。

系统涉及技术:ffmpeg,python,redis,celery,mysql,gfs.
ffmpeg:提供转码命令
Python:编程语言
redis:分布式任务队列提供者
celery:分布式任务执行着消费者
mysql:任务交互
gfs:文件系统
