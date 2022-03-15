#!/bin/bash

log_home=/opt/gmall0924/gmall-logger-0.0.1-SNAPSHOT.jar
jar=gmall-logger-0.0.1-SNAPSHOT.jar

case $1 in
"start")
echo "========在 hadoop162 启动 nginx ==============="
/opt/module/nginx/sbin/nginx
for i in hadoop162 hadoop163 hadoop164
do
  echo "========启动日志服务: $i==============="
  ssh $i "nohup java -Xms32m -Xmx64m -jar $log_home >/dev/null 2>&1  &"
  done
;;
"stop")
 echo "========在 hadoop162 停止 nginx ==============="
        /opt/module/nginx/sbin/nginx -s stop
        for i in hadoop162 hadoop163 hadoop164
        do
            echo "========关闭日志服务: $i==============="
            ssh $i "source /etc/profile; jps | grep gmall-logger-0.0.1-SNAPSHOT.jar | awk '{print \$1}'|xargs kill" >/dev/null 2>&1
        done
;;
*)
   echo 启动姿势不对, 请使用参数 start 启动日志服务, 使用参数 stop 停止服务
   ;;
esac