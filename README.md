# wallet

> 虚拟币钱包服务，转账/提现/充值/归集
>
>
> 完全实现与业务服务隔离，使用http服务相互调用

# 环境依赖

> go 1.20+

> MySQL 5.7

# 下载-打包

```shell
# 拉取代码
$ git clone https://github.com/lmxdawn/wallet.git
$ cd wallet

# 打包 (-tags "doc") 可选，加上可以运行swagger
$ go build [-tags "doc"]

# 直接运行示例配置
$ wallet -c config/config-example.yml

```

# 配置文件参数解释

|  参数名   | 描述  |
|  ----  | ----  |
| app.port  | 启动端口 |
| mysql.host  | MySQL的主机ip |
| mysql.port  | MySQL的端口 |
| mysql.db  | MySQL的数据库名 |
| mysql.user_name  | MySQL的登录用户 |
| mysql.password  | MySQL的登录密码 |

# 第三方库依赖

> log 日志 `github.com/rs/zerolog`

> 命令行工具 `github.com/urfave/cli`

> 配置文件 `github.com/jinzhu/configor`
> 
#Nginx的配置文件

```
server {
        listen        80;
        server_name  vaiwan.com;
        root   "你的前端静态目录";
        location / {
        }

        # go程序的代理
        location /api/ {
            proxy_pass http://127.0.0.1:10001/;
        }
}

```