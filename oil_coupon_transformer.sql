SET execution.checkpointing.interval = 10s;
SET table.local-time-zone = Asia/Shanghai;

-- 用户画像数据源
CREATE TABLE oil_coupon_qualification (
   id STRING, --id
   uid STRING,
   `type` INT,
   business_id STRING,
   create_time STRING,
   success_time STRING,
--    create_time TIMESTAMP_LTZ(3),
--    success_time TIMESTAMP_LTZ(3),
   PRIMARY KEY (id) NOT ENFORCED
 ) WITH (
     'connector' = 'filesystem',
     'path' = 'file:///Users/lizhongxiang/Documents/IdeaProjects/lzx/flink/oil_csv',
     'format' = 'csv'
 );