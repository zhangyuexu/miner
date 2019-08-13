## miner

#### 针对mysql多数据库之间数据同步工具

## 使用前提：
1. 数据库开启binlog，binlog_format=ROW
2. 下载并运行Canal
3. Canal地址：https://github.com/alibaba/canal
4. Canal配置修改：
```
1：文件：canal.properties
canal.destinations = miner
2：文件：instance.properties
canal.instance.master.address=数据库ip:端口
canal.instance.dbUsername=数据库账号
canal.instance.dbPassword=数据库密码
```

### 使用：
1. 下载项目
2. 打包
3. 修改配置 minerConfig.json
4. java -jar miner.jar -c minerConfig.json

### 配置介绍：
```
{
  //应用名称，线程池名称，CanalConnector.destination
  "appName": "miner",
  //线程池大小
  "threadPoolSize": 10,
  //CanalConnector参数配置
  "canal": {
    "ip": "10.168.29.215",
    "port": 11111,
    "user": "",
    "password": ""
  },
  //目标数据库配置，可配置多个
  "dataSource": {
  
    //数据库：xxx
    "xxx": {
      "driverClass": "com.mysql.jdbc.Driver",
      "url": "jdbc:mysql://xxx:3306/xxx?useUnicode=true&characterEncoding=utf-8&useSSL=false",
      "username": "xxx",
      "password": "xxx"
    }
  },
  
  // 同步任务，可以配置多个
  "jobs": [
    
    //任务1：
    {
      //实现类(单表的简单同步)
      "className": "com.hebaibai.miner.job.TableSyncJob",
      //目标数据库，对应dataSource节点的xxx数据库配置
      "dataSource": "xxx",
      //属性配置
      "prop": {
        //来源数据库
        "database": "pay",
        //来源表
        "table": "p_pay_msg",
        //目标表
        "target": "member",
        //字段转换，key：来源标；value：目标表
        "mapping": {
          "id": "id",
          "mch_code": "mCode",
          "send_type": "mName",
          "order_id": "phone",
          "created_time": "create_time",
          "creator": "remark"
        }
      }
    }
  ]
}
```

### 扩展
1. 继承com.hebaibai.miner.job.Job
2. 在minerConfig.json修改配置（dataSource，className必填）