#!/bin/bash

# 定义一个函数来启动 Django 服务器
start_server() {
  echo "Starting server..."
  nohup python3 -u manage.py runserver 0.0.0.0:8081 > $log_filename 2>&1 &
}

# 定义一个函数来停止 Django 服务器
stop_server() {
  echo "Stopping server..."
  pid=$(lsof -t -i:8081)
  if [ ! -z "$pid" ]; then
    kill -9 $pid
  fi
}

# 获取当前系统时间，并格式化为LOG+时间.log
current_time=$(date +"%Y-%m-%d_%H-%M-%S")
log_filename="LOG/backend.log"

# 启动服务器
start_server
echo "Server started and logs will be saved to $log_filename"

# 无限循环，每分钟检查一次
while true; do
  # 拉取最新代码
  git fetch origin

  # 检查当前分支与远程分支的差异
  status=$(git status -uno | grep 'Your branch is behind')
  if [ ! -z "$status" ]; then
    # 停止服务器
    stop_server

    # 执行 git pull
    git pull

    # 重新启动服务器
    start_server
    echo "Server restarted due to repository update"
  fi

  # 等待 60 秒
  sleep 60
done
