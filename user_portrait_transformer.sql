SET execution.checkpointing.interval = 10s;
SET table.local-time-zone = Asia/Shanghai;

-- 用户画像数据源
CREATE TABLE user_portrait (
   _id STRING, --id
   distinct_id STRING, --唯一id
   create_time TIMESTAMP_LTZ(3),
   update_time TIMESTAMP_LTZ(3),
   last_utm_campaign STRING, --最后活跃渠道
   device_name_list ARRAY<STRING>,--设备类型
   has_email BOOLEAN,
   has_phone BOOLEAN,
   signup_platform ARRAY<INT>,
   user_name STRING,
   is_login INT,
   signup_time INT,
   travel_tags MAP<STRING,INT>,
   PRIMARY KEY (_id) NOT ENFORCED
 ) WITH (
     'connector' = 'mongodb-cdc',
     'hosts' = 'localhost:27027',
     'username' = 'flinkuser',
     'password' = 'flinkpw',
     'database' = 'membership',
     'collection' = 'user_portrait_back',
     'batch.size' = '1000',
     'poll.max.batch.size' = '1000',
     'poll.await.time.ms' = '10000',
     'heartbeat.interval.ms' = '5000'
 );

-- es 输出表
CREATE TABLE es_user_portrait (
   distinct_id STRING, --唯一id
   create_time TIMESTAMP_LTZ(3),
   update_time TIMESTAMP_LTZ(3),
   last_utm_campaign STRING, --最后活跃渠道
   device_name_list ARRAY<STRING>,--设备类型
   has_email BOOLEAN,
   has_phone BOOLEAN,
   signup_platform ARRAY<INT>,
   user_name STRING,
   is_login INT,
   signup_time INT,
   travel_tags MAP<STRING,INT>,
--    `@timestamp` TIMESTAMP_LTZ(3),
   PRIMARY KEY (distinct_id) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://localhost:9200',
     'index' = 'es_user_portrait',
     'username' = 'elastic',
     'password' = 'changeme',
     'sink.bulk-flush.interval' = '1s',
     'sink.bulk-flush.backoff.strategy' = 'CONSTANT',
     'sink.bulk-flush.backoff.max-retries' = '3',
     'sink.bulk-flush.backoff.delay' = '3s',
     'failure-handler' = 'retry-rejected',
     'sink.flush-on-checkpoint' = 'false'
 );

-- 输入数据
INSERT INTO es_user_portrait
 SELECT    u.distinct_id,
           u.create_time,
           u.update_time,
           u.last_utm_campaign,
           u.device_name_list,
           u.has_email,
           u.has_phone,
           u.signup_platform,
           u.user_name,
           u.is_login,
           u.signup_time,
           u.travel_tags
--            CURRENT_TIMESTAMP(u.create_time) as `@timestamp`
   FROM user_portrait AS u;
