#!/bin/bash

# 脚本名称
SCRIPT_NAME="authors_rcv.py"

# 查找并杀死之前的脚本进程
pkill -f $SCRIPT_NAME

# 等待一小段时间确保进程已被杀死
sleep 2

# 重新启动脚本
nohup python3 -u $SCRIPT_NAME > LOG/output1.log 2>&1 &

echo "Script $SCRIPT_NAME has been restarted."