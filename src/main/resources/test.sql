SHOW variables LIKE '%time_zone%';
SHOW ROUTINE LOAD\G;

CREATE TABLE IF NOT EXISTS `ods_match_event_app_log` (
    `game_code` VARCHAR(10) NOT NULL COMMENT "游戏代号",
    `distinct_id` VARCHAR(64) NOT NULL COMMENT "设备唯一id",
    `account_id` VARCHAR(64) NULL COMMENT "当前登录账号",
    `account_infos` VARCHAR(65533) NULL COMMENT "该设备登录过的账号所有信息是一个jsonstring {'1_1111':{'register_time':'2022-01-02 00:00:00','is_ad':1}}",
    `ad_type` TINYINT(4) NULL COMMENT "判断当前账号类型 1 广告新增；2 广告回流； 3 自然量",
    `ad_plat` VARCHAR(20) NULL COMMENT "媒体平台",
    `ad_time` DATETIME NULL COMMENT "用户点击广告的时间",
    `ad_properties` VARCHAR(65533) NULL COMMENT "广告参数",
    `last_active_time` DATETIME NULL COMMENT "最新的用户行为事件时间",
    `last_active_event` VARCHAR(65533) NULL COMMENT "最新的用户行为事件名",
    `device_properties` VARCHAR(65533) NULL COMMENT "设备信息",
    `last_event_properties` VARCHAR(65533) NULL COMMENT "最新用户行为事件的参数",
    `state_properties` VARCHAR(65533) NULL COMMENT "状态信息 设备信息 是否是广告设备",
    `monitor_id` INT(11) NULL COMMENT "监测链接id",
    `package_id` VARCHAR(64) NULL COMMENT "包id",
    `stat_day` DATE NULL DEFAULT "1970-01-01" COMMENT "日期",
    `update_at` DATETIME NULL COMMENT "更新时间戳"
    ) ENGINE=OLAP
    DUPLICATE KEY(`game_code`, `distinct_id`, `account_id`)
    COMMENT "app广告匹配输出数据"
    PARTITION BY RANGE(`stat_day`)
(
    PARTITION p20220716 VALUES [('2022-07-16'), ('2022-07-17')),
    PARTITION p20220717 VALUES [('2022-07-17'), ('2022-07-18')),
    PARTITION p20220718 VALUES [('2022-07-18'), ('2022-07-19')),
    PARTITION p20220719 VALUES [('2022-07-19'), ('2022-07-20')),
    PARTITION p20220720 VALUES [('2022-07-20'), ('2022-07-21')),
    PARTITION p20220721 VALUES [('2022-07-21'), ('2022-07-22')),
    PARTITION p20220722 VALUES [('2022-07-22'), ('2022-07-23')),
    PARTITION p20220723 VALUES [('2022-07-23'), ('2022-07-24')),
    PARTITION p20220724 VALUES [('2022-07-24'), ('2022-07-25')),
    PARTITION p20220725 VALUES [('2022-07-25'), ('2022-07-26')),
    PARTITION p20220726 VALUES [('2022-07-26'), ('2022-07-27')),
    PARTITION p20220727 VALUES [('2022-07-27'), ('2022-07-28')),
    PARTITION p20220728 VALUES [('2022-07-28'), ('2022-07-29')),
    PARTITION p20220729 VALUES [('2022-07-29'), ('2022-07-30')),
    PARTITION p20220730 VALUES [('2022-07-30'), ('2022-07-31')),
    PARTITION p20220731 VALUES [('2022-07-31'), ('2022-08-01')))
    DISTRIBUTED BY HASH(`account_id`) BUCKETS 32
    PROPERTIES (
                   "replication_num" = "2",
                   "dynamic_partition.ENABLE" = "TRUE",
                   "dynamic_partition.time_unit" = "DAY",
                   "dynamic_partition.time_zone" = "Asia/Dubai",
                   "dynamic_partition.start" = "-365",
                   "dynamic_partition.END" = "3",
                   "dynamic_partition.prefix" = "p",
                   "dynamic_partition.BUCKETS" = "32",
                   "in_memory" = "FALSE",
                   "storage_format" = "DEFAULT"
               );



STOP ROUTINE LOAD FOR ods_match_event_app_log;
CREATE ROUTINE LOAD ad.ods_match_event_app_log ON ods_match_event_app_log
COLUMNS(game_code,distinct_id,account_id,account_infos,ad_type,ad_plat,ad_time,ad_properties,last_active_time,last_active_event,device_properties,last_event_properties,state_properties,monitor_id,package_id,stat_day=TO_DATE(last_active_time),update_at=CURRENT_TIMESTAMP())
PROPERTIES
(
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "10",
    "strict_mode" = "FALSE",
    "timezone" = "+08:00",
    "format" = "json",
    "jsonpaths" = "[\"$.game_code\",\"$.distinct_id\",\"$.account_id\",\"$.account_infos\",\"$.ad_type\",\"$.ad_plat\",\"$.ad_time\",\"$.ad_properties\",\"$.last_active_time\",\"$.last_active_event\",\"$.device_properties\",\"$.last_event_properties\",\"$.state_properties\",\"$.monitor_id\",\"$.package_id\"]",
    "strip_outer_array" = "FALSE",
    "max_error_number" = "1000"
)
FROM KAFKA
(
    "kafka_broker_list" = "emr_kafka1:9092,emr_kafka2:9092,emr_kafka3:9092",
    "kafka_topic" = "client_ad_match_result"
);




CREATE TABLE IF NOT EXISTS `ods_match_event_mini_log` (
    `game_code` VARCHAR(10) NOT NULL COMMENT "游戏代号",
    `distinct_id` VARCHAR(64)  NULL COMMENT "设备唯一id",
    `account_id` VARCHAR(64) NOT NULL COMMENT "当前登录账号",
    `account_infos` VARCHAR(65533) NULL COMMENT "该设备登录过的账号所有信息是一个jsonstring {'1_1111':{'register_time':'2022-01-02 00:00:00','is_ad':1}}",
    `ad_type` TINYINT(4) NULL COMMENT "判断当前账号类型 1 广告新增；2 广告回流； 3 自然量",
    `ad_plat` VARCHAR(20) NULL COMMENT "媒体平台",
    `ad_time` DATETIME NULL COMMENT "用户点击广告的时间",
    `ad_properties` VARCHAR(65533) NULL COMMENT "广告参数",
    `last_active_time` DATETIME NULL COMMENT "最新的用户行为事件时间",
    `last_active_event` VARCHAR(65533) NULL COMMENT "最新的用户行为事件名",
    `device_properties` VARCHAR(65533) NULL COMMENT "设备信息",
    `last_event_properties` VARCHAR(65533) NULL COMMENT "最新用户行为事件的参数",
    `state_properties` VARCHAR(65533) NULL COMMENT "状态信息 设备信息 是否是广告设备",
    `monitor_id` INT(11) NULL COMMENT "监测链接id",
    `package_id` VARCHAR(64) NULL COMMENT "包id",
    `stat_day` DATE NULL DEFAULT "1970-01-01" COMMENT "日期",
    `update_at` DATETIME NULL COMMENT "更新时间戳"
    ) ENGINE=OLAP
    DUPLICATE KEY(`game_code`, `distinct_id`, `account_id`)
    COMMENT "小游戏广告匹配输出数据"
    PARTITION BY RANGE(`stat_day`)
(
    PARTITION p20220716 VALUES [('2022-07-16'), ('2022-07-17')),
    PARTITION p20220717 VALUES [('2022-07-17'), ('2022-07-18')),
    PARTITION p20220718 VALUES [('2022-07-18'), ('2022-07-19')),
    PARTITION p20220719 VALUES [('2022-07-19'), ('2022-07-20')),
    PARTITION p20220720 VALUES [('2022-07-20'), ('2022-07-21')),
    PARTITION p20220721 VALUES [('2022-07-21'), ('2022-07-22')),
    PARTITION p20220722 VALUES [('2022-07-22'), ('2022-07-23')),
    PARTITION p20220723 VALUES [('2022-07-23'), ('2022-07-24')),
    PARTITION p20220724 VALUES [('2022-07-24'), ('2022-07-25')),
    PARTITION p20220725 VALUES [('2022-07-25'), ('2022-07-26')),
    PARTITION p20220726 VALUES [('2022-07-26'), ('2022-07-27')),
    PARTITION p20220727 VALUES [('2022-07-27'), ('2022-07-28')),
    PARTITION p20220728 VALUES [('2022-07-28'), ('2022-07-29')),
    PARTITION p20220729 VALUES [('2022-07-29'), ('2022-07-30')),
    PARTITION p20220730 VALUES [('2022-07-30'), ('2022-07-31')),
    PARTITION p20220731 VALUES [('2022-07-31'), ('2022-08-01')))
    DISTRIBUTED BY HASH(`account_id`) BUCKETS 32
    PROPERTIES (
                   "replication_num" = "2",
                   "dynamic_partition.ENABLE" = "TRUE",
                   "dynamic_partition.time_unit" = "DAY",
                   "dynamic_partition.time_zone" = "Asia/Dubai",
                   "dynamic_partition.start" = "-365",
                   "dynamic_partition.END" = "3",
                   "dynamic_partition.prefix" = "p",
                   "dynamic_partition.BUCKETS" = "32",
                   "in_memory" = "FALSE",
                   "storage_format" = "DEFAULT"
               );




STOP ROUTINE LOAD FOR ods_match_event_mini_log;
CREATE ROUTINE LOAD ad.ods_match_event_mini_log ON ods_match_event_mini_log
COLUMNS(game_code,distinct_id,account_id,account_infos,ad_type,ad_plat,ad_time,ad_properties,last_active_time,last_active_event,device_properties,last_event_properties,state_properties,monitor_id,package_id,stat_day=TO_DATE(last_active_time),update_at=CURRENT_TIMESTAMP())
PROPERTIES
(
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "10",
    "strict_mode" = "FALSE",
    "timezone" = "+08:00",
    "format" = "json",
    "jsonpaths" = "[\"$.game_code\",\"$.distinct_id\",\"$.account_id\",\"$.account_infos\",\"$.ad_type\",\"$.ad_plat\",\"$.ad_time\",\"$.ad_properties\",\"$.last_active_time\",\"$.last_active_event\",\"$.device_properties\",\"$.last_event_properties\",\"$.state_properties\",\"$.monitor_id\",\"$.package_id\"]",
    "strip_outer_array" = "FALSE",
    "max_error_number" = "1000"
)
FROM KAFKA
(
    "kafka_broker_list" = "emr_kafka1:9092,emr_kafka2:9092,emr_kafka3:9092",
    "kafka_topic" = "mini_ad_match_result"
);




    "property.GROUP.id" = "mini_ad_match_result_starrock",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"

WHERE event_time > "2022-01-01 00:00:00",


SHOW ROUTINE LOAD TASK WHERE Jobname="ods_match_event_mini_log" \G;
SHOW ROUTINE LOAD TASK WHERE Jobname="ods_match_event_app_log" \G;






--创建视图来查看数据  和实时的报表格式一模一样既可   split_index换成split_part   json_value换成get_json_string   get_json_int  get_json_double
-- 此处 last_pay_amount 需要后期验证
DROP VIEW ods_match_event_log_view;
CREATE VIEW ods_match_event_log_view AS
SELECT --ad_plat 1为字节系 2为腾讯系
       TO_DATE(date_format(last_active_time, 'yyyy-MM-dd')) stat_day ,
       CASE WHEN ad_plat='2'  THEN  split_part(get_json_string(ad_properties,'$.weixinadinfo'),'.', 1)  ELSE  get_json_string(ad_properties,'$.creative_id') END   ad_uniqid,--微信weixinadinfo 巨量 creative_id
       'mp' client,
       CASE WHEN  lower(get_json_string(state_properties,'$.device_os'))  LIKE '%android%'  THEN  'android ' WHEN  lower(get_json_string(state_properties,'$.device_os'))  LIKE '%ios%' THEN 'ios'  ELSE  'unknown' END os,--目前是从 事件中提取 后面都是从 state_properties 提取
       game_code,
       distinct_id,
       account_id,
       ad_type,
       ad_plat,
       ad_time,
       get_json_string(ad_properties,'$.campaign_id') campaign_id,
       CASE WHEN ad_plat='2'  THEN  split_part(get_json_string(ad_properties,'$.weixinadinfo'),'.', 1)  ELSE  get_json_string(ad_properties,'$.ad_id') END    ad_id,
       get_json_string(ad_properties,'$.creative_id') adcreative_id,
       last_active_time,
       last_active_event,
       get_json_string(state_properties,'$.first_time_active_time') first_time_active_time,
       get_json_int(state_properties,'$.match_state') match_state,
       monitor_id,
       package_id,
       get_json_string(get_json_string(account_infos,CONCAT('$.',account_id)),'$.register_time') register_time,
       get_json_string(get_json_string(account_infos,CONCAT('$.',account_id)),'$.first_pay_time') first_pay_time,
       get_json_double(get_json_string(account_infos,CONCAT('$.',account_id)),'$.last_pay_amount') last_pay_amount
        ,ad_properties,last_event_properties,account_infos,device_properties,state_properties,update_at
FROM  ods_match_event_mini_log
WHERE ad_plat <> ''  AND  last_active_event IS NOT NULL  AND  last_active_time IS NOT NULL

UNION ALL

SELECT
    TO_DATE(date_format(last_active_time, 'yyyy-MM-dd')) stat_day ,
    CASE WHEN ad_plat='mp'  THEN  get_json_string(ad_properties,'$.ad_id')  ELSE  get_json_string(ad_properties,'$.cid') END ad_uniqid,  --广点通ad_id 巨量cid
    'app' client,
    CASE WHEN  lower(get_json_string(last_event_properties,'$.device_os'))  LIKE '%android%'  THEN  'android ' WHEN  lower(get_json_string(last_event_properties,'$.device_os'))  LIKE '%ios%' THEN 'ios'  ELSE  'unknown' END os,
    game_code,
    distinct_id,
    account_id,
    ad_type,
    ad_plat,
    ad_time,
    CASE WHEN ad_plat='mp'  THEN  get_json_string(ad_properties,'$.campaign_id')  ELSE  get_json_string(ad_properties,'$.campaign_id') END campaign_id,
    CASE WHEN ad_plat='mp'  THEN  get_json_string(ad_properties,'$.adgroup_id')  ELSE  get_json_string(ad_properties,'$.aid') END ad_id,
    CASE WHEN ad_plat='mp'  THEN  get_json_string(ad_properties,'$.ad_id')  ELSE  get_json_string(ad_properties,'$.cid') END  adcreative_id,
    last_active_time,
    last_active_event,  --后期事件名 伟豪那边控制  我这边可以不用处理了
    get_json_string(state_properties,'$.first_time_active_time') first_time_active_time,
    get_json_int(state_properties,'$.match_state') match_state,
    monitor_id,
    package_id,
    get_json_string(get_json_string(account_infos,CONCAT('$.',account_id)),'$.register_time') register_time,
    get_json_string(get_json_string(account_infos,CONCAT('$.',account_id)),'$.first_pay_time') first_pay_time,
    get_json_double(get_json_string(account_infos,CONCAT('$.',account_id)),'$.last_pay_amount') last_pay_amount
        ,ad_properties,last_event_properties,account_infos,device_properties,state_properties,update_at
FROM `ods_match_event_app_log`
WHERE ad_plat <> ''  AND  last_active_event IS NOT NULL  AND  last_active_time IS NOT NULL
;

--广告取值情况
SELECT * FROM (
                  SELECT game_code,client ,ad_plat,ad_properties,last_active_time,row_number() OVER (PARTITION BY game_code,client ,ad_plat ORDER BY last_active_time DESC) rank
                  FROM ods_match_event_log_view
                  WHERE ad_plat <> ''
              ) tmp WHERE rank=1 \G
;

INSERT INTO  ods_match_event_log_distinct
SELECT
    game_code               ,
    distinct_id            ,
    account_id             ,
    stat_day               ,
    client                 ,
    os                     ,
    ad_type                ,
    ad_plat                ,
    ad_time                ,
    campaign_id            ,
    ad_id                  ,
    adcreative_id          ,
    ad_uniqid              ,
    last_active_time       ,
    last_active_event      ,
    first_time_active_time ,
    match_state            ,
    monitor_id             ,
    package_id             ,
    register_time          ,
    first_pay_time         ,
    last_pay_amount        ,
    ad_properties          ,
    last_event_properties  ,
    account_infos          ,
    device_properties      ,
    state_properties        ,
    update_at
FROM (
         SELECT  *,row_number() OVER (PARTITION BY game_code,distinct_id,account_id,last_active_time,last_active_event ORDER BY update_at DESC) AS rank
         FROM ods_match_event_log_view
         WHERE  ad_uniqid IS NOT NULL   AND stat_day='${yesterday}'
     ) tmp WHERE rank =1
;























DROP TABLE ods_match_event_log_distinct;
CREATE TABLE `ods_match_event_log_distinct` (
                                                `game_code` VARCHAR(20) NULL DEFAULT "" COMMENT "主游戏",
                                                `distinct_id` VARCHAR(100) NULL DEFAULT "" COMMENT "设备唯一id",
                                                `account_id` VARCHAR(100) NULL DEFAULT "" COMMENT "游戏账号",
                                                `stat_day` DATE NULL DEFAULT "1970-01-01" COMMENT "日期",
                                                `client` VARCHAR(20) NULL DEFAULT "" COMMENT "客户端类型",
                                                `os` VARCHAR(20) NULL COMMENT "android或者ios",
                                                `ad_type` BIGINT(20) NULL COMMENT "判断当前账号类型 1 广告新增；2 广告回流； 3 自然量",
                                                `ad_plat` VARCHAR(20) NULL COMMENT "媒体平台",
                                                `ad_time` VARCHAR(20) NULL COMMENT "广告回传时间",
                                                `campaign_id` VARCHAR(40) NULL COMMENT "广告组id",
                                                `ad_id` VARCHAR(100) NULL DEFAULT "" COMMENT "广告ID",
                                                `adcreative_id` VARCHAR(40) NULL COMMENT "广告创意id",
                                                `ad_uniqid` VARCHAR(40) NULL COMMENT "广告最小的唯一id 小程序 目前用ad_id  app用creative_id",
                                                `last_active_time` DATETIME NULL COMMENT "设备最后一次活跃时间",
                                                `last_active_event` VARCHAR(65533) NULL COMMENT "设备最后���次活跃事件  枚举 install_active  register log_in",
                                                `first_time_active_time` VARCHAR(20) NULL COMMENT "第一次open时间",
                                                `match_state` BIGINT(20) NULL COMMENT "1代表Redis 2代表用户表 3代表首次匹配成功 4代表首次但是匹配不成功",
                                                `monitor_id` BIGINT(20) NULL COMMENT "监测序号id",
                                                `package_id` VARCHAR(30) NULL COMMENT "包id",
                                                `register_time` VARCHAR(30) NULL COMMENT "注册时间",
                                                `first_pay_time` VARCHAR(30) NULL COMMENT "首次付费时间",
                                                `last_pay_amount` DOUBLE NULL COMMENT "当前付费的金额",
                                                `ad_properties` VARCHAR(1000) NULL COMMENT "广告回传参数",
                                                `last_event_properties` VARCHAR(1000) NULL COMMENT " 当前��备用户行为信息json",
                                                `account_infos` VARCHAR(1000) NULL COMMENT "该设备登录过的账号所有信息",
                                                `device_properties` VARCHAR(1000) NULL COMMENT "设备信息json",
                                                `state_properties` VARCHAR(200) NULL COMMENT "匹配情况",
                                                `update_at` DATETIME NULL COMMENT "更新时间戳"
) ENGINE=OLAP
DUPLICATE KEY(`game_code`, `distinct_id`, `account_id`)
COMMENT "广告匹配输出数据"
PARTITION BY RANGE(`stat_day`)
(PARTITION p20220625 VALUES [('2022-06-25'), ('2022-06-26')),
PARTITION p20220626 VALUES [('2022-06-26'), ('2022-06-27')),
PARTITION p20220627 VALUES [('2022-06-27'), ('2022-06-28')),
PARTITION p20220628 VALUES [('2022-06-28'), ('2022-06-29')),
PARTITION p20220629 VALUES [('2022-06-29'), ('2022-06-30')),
PARTITION p20220630 VALUES [('2022-06-30'), ('2022-07-01')),
PARTITION p20220701 VALUES [('2022-07-01'), ('2022-07-02')),
PARTITION p20220702 VALUES [('2022-07-02'), ('2022-07-03')),
PARTITION p20220703 VALUES [('2022-07-03'), ('2022-07-04')),
PARTITION p20220704 VALUES [('2022-07-04'), ('2022-07-05')),
PARTITION p20220705 VALUES [('2022-07-05'), ('2022-07-06')),
PARTITION p20220706 VALUES [('2022-07-06'), ('2022-07-07')),
PARTITION p20220707 VALUES [('2022-07-07'), ('2022-07-08')),
PARTITION p20220708 VALUES [('2022-07-08'), ('2022-07-09')),
PARTITION p20220709 VALUES [('2022-07-09'), ('2022-07-10')),
PARTITION p20220710 VALUES [('2022-07-10'), ('2022-07-11')),
PARTITION p20220711 VALUES [('2022-07-11'), ('2022-07-12')),
PARTITION p20220712 VALUES [('2022-07-12'), ('2022-07-13')),
PARTITION p20220713 VALUES [('2022-07-13'), ('2022-07-14')),
PARTITION p20220714 VALUES [('2022-07-14'), ('2022-07-15')),
PARTITION p20220715 VALUES [('2022-07-15'), ('2022-07-16')),
PARTITION p20220716 VALUES [('2022-07-16'), ('2022-07-17')),
PARTITION p20220717 VALUES [('2022-07-17'), ('2022-07-18')),
PARTITION p20220718 VALUES [('2022-07-18'), ('2022-07-19')),
PARTITION p20220719 VALUES [('2022-07-19'), ('2022-07-20')),
PARTITION p20220720 VALUES [('2022-07-20'), ('2022-07-21')),
PARTITION p20220721 VALUES [('2022-07-21'), ('2022-07-22')),
PARTITION p20220722 VALUES [('2022-07-22'), ('2022-07-23')),
PARTITION p20220723 VALUES [('2022-07-23'), ('2022-07-24')),
PARTITION p20220724 VALUES [('2022-07-24'), ('2022-07-25')),
PARTITION p20220725 VALUES [('2022-07-25'), ('2022-07-26')),
PARTITION p20220726 VALUES [('2022-07-26'), ('2022-07-27')),
PARTITION p20220727 VALUES [('2022-07-27'), ('2022-07-28')),
PARTITION p20220728 VALUES [('2022-07-28'), ('2022-07-29')),
PARTITION p20220729 VALUES [('2022-07-29'), ('2022-07-30')),
PARTITION p20220730 VALUES [('2022-07-30'), ('2022-07-31')),
PARTITION p20220731 VALUES [('2022-07-31'), ('2022-08-01')))
DISTRIBUTED BY HASH(`account_id`) BUCKETS 32
PROPERTIES (
"replication_num" = "1",
"dynamic_partition.ENABLE" = "TRUE",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Asia/Dubai",
"dynamic_partition.start" = "-31",
"dynamic_partition.END" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.BUCKETS" = "32",
"in_memory" = "FALSE",
"storage_format" = "DEFAULT"
);





`kafka_event_time` DATETIME NULL COMMENT "kafka的事件时间",
  `kafka_partition` BIGINT(20) NULL COMMENT "kafka的partition",
  `kafka_offset` BIGINT(20) NULL COMMENT "kafka的offset"

---洗一张 去重表
INSERT INTO  ods_match_event_log_distinct
SELECT
    game_code               ,
    distinct_id            ,
    account_id             ,
    stat_day               ,
    client                 ,
    os                     ,
    ad_type                ,
    ad_plat                ,
    ad_time                ,
    campaign_id            ,
    ad_id                  ,
    adcreative_id          ,
    ad_uniqid              ,
    last_active_time       ,
    last_active_event      ,
    first_time_active_time ,
    match_state            ,
    monitor_id             ,
    package_id             ,
    register_time          ,
    first_pay_time         ,
    last_pay_amount        ,
    ad_properties          ,
    last_event_properties  ,
    account_infos          ,
    device_properties      ,
    state_properties        ,
    kafka_event_time       ,
    kafka_partition        ,
    kafka_offset
FROM (
         SELECT  *,row_number() OVER (PARTITION BY kafka_partition,kafka_offset ORDER BY update_at DESC) AS rank FROM ods_match_event_log
         WHERE  ad_uniqid IS NOT NULL   AND stat_day='${yesterday}'
     ) tmp WHERE rank =1
;



