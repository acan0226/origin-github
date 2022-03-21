#!/bin/bash

log_home=/opt/gmall210924
jar=gmall-logger-0.0.1-SNAPSHOT.jar
nginx=/opt/module/nginx/sbin/nginx
case $1 in
"start")
    # -z 是否为空
    if [[ -z "`pgrep nginx`" ]]; then
         echo "在 hadoop162 上启动 nginx..."
         $nginx
    else
        echo "在 hadoop162 上 nginx 已经启动, 无序重复启动."
    fi
    for host in hadoop162 hadoop163 hadoop164 ; do
        echo "在 $host 上启动日志服务器..."
        ssh $host "nohup java -jar $log_home/$jar 1>$log_home/log.out  2>$log_home/log.error &"
    done
   ;;
"stop")
    echo "在 hadoop162 上停止 nginx..."
    $nginx -s stop

    for host in hadoop162 hadoop163 hadoop164 ; do
        echo "在 $host 上停止日志服务器..."
        ssh $host "jps | awk '/$jar/{print \$1}' | xargs kill -9"

    done

   ;;
*)
    echo "你日志采集系统使用的姿势不对:"
    echo " logger.sh start 启动日志采集"
    echo " logger.sh stop  停止日志采集"
   ;;
esac

# 在hadoop162启动一个nginx

# 分别在hadoop162-164启动是日志服务器


