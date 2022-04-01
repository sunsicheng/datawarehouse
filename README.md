# datawarehouse
Flink实时数仓，基于flink1.12
==

1.启动集群步骤
--
1.1 sudo systemctl start docker
 
1.2 /home/atguigu/docker_bigdata/contains.sh start



2.启动服务器
--
启动hadoop集群    hadoop.sh start

启动zookeeper集群    zk.sh start

启动kafka集群 kafka.sh start

启动nginx和日志服务器  log_cluster.sh start

生产日志数据 java -jar gmall2020-mock-log-2020-12-18.jar

启动maxwell maxwell.sh start

生产业务数据 java -jar gmall2020-mock-db-2020-12-23.jar

