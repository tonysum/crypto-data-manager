#!/bin/bash
set -e  # 遇到错误立即退出

# ====================== 配置项（可根据需求修改）======================
PG_VERSION="15"               # PostgreSQL版本
DB_NAME="crypto_data"         # 要创建的数据库名
DB_USER="crypto_user"         # 数据库用户（自定义）
DB_PASSWORD="Crypto@891109"   # 数据库密码（建议替换为强密码）
PG_PORT="5432"                # PostgreSQL端口
# ====================================================================

# 第一步：更新系统并安装依赖
echo -e "\033[32m[1/6] 升级系统包并安装依赖...\033[0m"
sudo apt update && sudo apt upgrade -y
sudo apt install -y wget gnupg2 lsb-release

# 第二步：添加PostgreSQL官方源（确保安装最新15版本）
echo -e "\033[32m[2/6] 添加PostgreSQL官方软件源...\033[0m"
# 导入PG官方GPG密钥
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
# 添加源到sources.list.d
echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list

# 第三步：安装PostgreSQL 15
echo -e "\033[32m[3/6] 安装PostgreSQL $PG_VERSION...\033[0m"
sudo apt update
sudo apt install -y postgresql-$PG_VERSION postgresql-contrib-$PG_VERSION

# 第四步：启动并设置开机自启
echo -e "\033[32m[4/6] 启动PostgreSQL服务并设置开机自启...\033[0m"
sudo systemctl start postgresql@$PG_VERSION-main
sudo systemctl enable postgresql@$PG_VERSION-main
# 验证服务状态
if ! sudo systemctl is-active --quiet postgresql@$PG_VERSION-main; then
    echo -e "\033[31m[错误] PostgreSQL $PG_VERSION 服务启动失败！\033[0m"
    exit 1
fi

# 第五步：创建数据库和用户（通过postgres系统用户执行）
echo -e "\033[32m[5/6] 创建 $DB_NAME 数据库和 $DB_USER 用户...\033[0m"
sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';"
sudo -u postgres psql -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"
# 赋予用户数据库全权限
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;"

# 第六步：配置远程访问（可选，如需外部连接则启用）
echo -e "\033[32m[6/6] 配置PostgreSQL（可选：开启远程访问）...\033[0m"
# 修改postgresql.conf：监听所有IP
PG_CONF="/etc/postgresql/$PG_VERSION/main/postgresql.conf"
sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/g" $PG_CONF

# 修改pg_hba.conf：允许密码认证
PG_HBA="/etc/postgresql/$PG_VERSION/main/pg_hba.conf"
# 添加远程访问规则（末尾追加）
echo "host    all             all             0.0.0.0/0               scram-sha-256" | sudo tee -a $PG_HBA

# 重启服务使配置生效
sudo systemctl restart postgresql@$PG_VERSION-main

# 验证结果
echo -e "\033[32m==================== 安装完成 ====================\033[0m"
echo -e "PostgreSQL $PG_VERSION 安装路径：/usr/lib/postgresql/$PG_VERSION/"
echo -e "数据目录：/var/lib/postgresql/$PG_VERSION/main/"
echo -e "创建的数据库：$DB_NAME"
echo -e "数据库用户：$DB_USER"
echo -e "数据库密码：$DB_PASSWORD"
echo -e "连接测试命令：psql -U $DB_USER -d $DB_NAME -h 127.0.0.1 -p $PG_PORT"
echo -e "\033[33m注意：若开启了远程访问，请确保服务器防火墙放行 $PG_PORT 端口！\033[0m"