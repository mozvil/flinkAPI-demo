tar -zxvf flink-1.15.2-bin-scala_2.12.tgz -C /opt/

vi /etc/profile.d/my-env.sh 
------------------------------------
export FLINK_HOME=/opt/flink-1.15.2
...
export PATH=$PATH:$FLINK_HOME/bin
------------------------------------

source /etc/profile

vi /opt/flink-1.15.2/conf/flink-conf.yaml
-----------------------------------------
localhost全部替换为hadoop100

vi /opt/flink-1.15.2/conf/masters
-----------------------------------------
localhost替换为hadoop100

vi /opt/flink-1.15.2/conf/workers
--------------------------------
hadoop100
hadoop101

# 启动
start-cluster.sh

http://192.168.20.100:8081/#/overview
http://hadoop100:8081/#/overview

# 停止
stop-cluster.sh

standalone集群配置
------------------------

vi /opt/flink-1.15.2/conf/flink-conf.yaml
-----------------------------------------
# master地址(如果要启动2个jobmanager此处要配置各自的服务器地址)
jobmanager.rpc.address: hadoop100
 
# master端口
jobmanager.rpc.port: 6123

# master地址绑定设置（master节点参数）
jobmanager.bind-host: 0.0.0.0

# worker地址绑定设置
taskmanager.bind-host: 0.0.0.0

# worker地址（注意：三个worker节点的host不一样）
taskmanager.host: hadoop100
 
# worker槽位数设置
taskmanager.numberOfTaskSlots: 2
 
# 默认并行度
parallelism.default: 2

# WEB UI 端口（master节点参数）
rest.port: 8081

# WEB UI 管理地址
rest.address: hadoop100

# WEB UI 地址绑定设置，想让外部访问，可以设置具体的IP，或者直接设置成“0.0.0.0”（master节点参数）
rest.bind-address: 0.0.0.0

# Job文件目录（master节点参数）
web.upload.dir: /opt/flink-1.15.2/usrlib

# IO临时目录，默认：/tmp
io.tmp.dirs: /opt/flink-1.15.2/tmp

# 集群节点进程ID存放目录，默认：/tmp
env.pid.dir: /opt/flink-1.15.2/pids
-----------------------------------

vi /opt/flink-1.15.2/conf/masters
-----------------------------------------
hadoop100:8081
hadoop101:8081
-----------------------------------------

scp -r /opt/flink-1.15.2/ hadoop101:/opt/

# hadoop101配置环境变量
vi /etc/profile.d/my-env.sh 
------------------------------------
export FLINK_HOME=/opt/flink-1.15.2
...
export PATH=$PATH:$FLINK_HOME/bin
------------------------------------
source /etc/profile

yarn集群配置
# hadoop100 hadoop101 增加环境变量
vi /etc/profile.d/my-env.sh
---------------------------
...
export HADOOP_CLASSPATH=`hadoop classpath`
--------------------------------------------

source /etc/profile
# standalone集群启动
start-cluster.sh
# standalone集群停止
stop-cluster.sh


# session 集群操作
---------------------------------------
# session部署模式先要启动flink
# 参数 -s slot个数
# 参数 -jm job manager的堆内存大小
# 参数 -tm task manager的堆内存大小
# 参数 --detached 分离模式，启动完成立即断开(flink集群将一直运行在yarn上并且随时可以提交job)
yarn-session.sh -s 2 -jm 1024 -tm 1024 --detached

flink run demo-job-0.0.1-SNAPSHOT.jar


# 查询 flink job

[root@hadoop100 ~]# flink list
Waiting for response...
------------------ Running/Restarting Jobs -------------------
04.09.2022 15:41:08 : 68edd97eb119a0c0f5d263a0a167c347 : Flink Streaming Job (RUNNING)
--------------------------------------------------------------
No scheduled jobs.

# 停止job
[root@hadoop100 ~]# flink cancel 68edd97eb119a0c0f5d263a0a167c347
Cancelling job 68edd97eb119a0c0f5d263a0a167c347.
Cancelled job 68edd97eb119a0c0f5d263a0a167c347.
[root@hadoop100 ~]# flink list
Waiting for response...
No running jobs.
No scheduled jobs.

# 查询 yarn application
[root@hadoop100 ~]# yarn application --list
2022-09-04 15:47:54,091 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at hadoop101/192.168.20.101:8032
Total number of applications (application-types: [], states: [SUBMITTED, ACCEPTED, RUNNING] and tags: []):1
                Application-Id      Application-Name        Application-Type          User           Queue                   State             Final-State             Progress                        Tracking-URL
application_1662276446599_0001  Flink session cluster           Apache Flink          root         default                 RUNNING               UNDEFINED                 100%          http://192.168.20.100:8081

# 停止 yarn application
[root@hadoop100 ~]# yarn application -kill application_1662276446599_0001
2022-09-04 15:49:01,479 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at hadoop101/192.168.20.101:8032
Killing application application_1662276446599_0001
2022-09-04 15:49:02,116 INFO impl.YarnClientImpl: Killed application application_1662276446599_0001


# Flink Job 集群操作
---------------------------------------
# Flink Job模式提交job之前先将yarn-session和standalone集群关闭
# 参数 -t 运行模式 这里使用yarn-per-job(即yarn集群的per-job模式)
# 参数 -ys slot个数
# 参数 -ynm Yarn appliation的名字
# 参数 -yn task manager的数量(可不填)
# 参数 -yjm job manager的堆内存大小
# 参数 -ytm task manager的堆内存大小
# 参数 -c 指定jar包中的class全名，类名后面紧接着是jar文件(可不填)
# 参数 --detached 分离模式，启动完成立即断开
flink run -t yarn-per-job -ys 1 -ynm test-wordcount -yjm 1024 -ytm 1024 /root/Documents/demo-job-0.0.1-SNAPSHOT.jar

# 停止作业同时也停止了Flink在yarn上运行的application 
yarn application -kill application_1662310000147_0002


# Flink Application 集群操作(生产推荐)
-------------------------------------
flink run-application -t yarn-application \
-Djobmanager.memory.process.size=1024m \
-Dtaskmanager.memory.process.size=1024 \
-Dtaskmanager.numberOfTaskSlot=1 \
-Dparallelism.default=1 \
-Dyarn.application.name="flick-wc" \
/root/Documents/demo-job-0.0.1-SNAPSHOT.jar

Historyserver配置启动
--------------------------------------
# hadoop100 hadoop101 修改配置文件
vi /opt/flink-1.15.2/flink-conf.yaml
----------------------------------------------------------------
# Directory to upload completed jobs to. Add this directory to the list of
# monitored directories of the HistoryServer as well (see below).
jobmanager.archive.fs.dir: hdfs://hadoop100:8020/flink-completed-jobs/

# The address under which the web-based HistoryServer listens.
historyserver.web.address: hadoop100

# The port under which the web-based HistoryServer listens.
historyserver.web.port: 8082

# Comma separated list of directories to monitor for completed jobs.
historyserver.archive.fs.dir: hdfs://hadoop100:8020/flink-completed-jobs/

# Interval in milliseconds for refreshing the monitored directories.
historyserver.archive.fs.refresh-interval: 10000
---------------------------------------------------

historyserver.sh start
historyserver.sh stop
web: http://hadoop100:8082/