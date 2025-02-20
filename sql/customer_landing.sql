{\rtf1\ansi\ansicpg1252\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Monaco;}
{\colortbl;\red255\green255\blue255;\red13\green16\blue20;\red255\green255\blue255;}
{\*\expandedcolortbl;;\cssrgb\c5882\c7843\c10196;\cssrgb\c100000\c100000\c100000;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 CREATE EXTERNAL TABLE `customer_landing`(\
  `customername` string COMMENT 'from deserializer', \
  `email` string COMMENT 'from deserializer', \
  `phone` string COMMENT 'from deserializer', \
  `birthday` string COMMENT 'from deserializer', \
  `serialnumber` string COMMENT 'from deserializer', \
  `registrationdate` bigint COMMENT 'from deserializer', \
  `lastupdatedate` bigint COMMENT 'from deserializer', \
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', \
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', \
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer')\
ROW FORMAT SERDE \
  'org.openx.data.jsonserde.JsonSerDe' \
WITH SERDEPROPERTIES ( \
  'paths'='birthDay,customerName,email,lastUpdateDate,phone,registrationDate,serialNumber,shareWithFriendsAsOfDate,shareWithPublicAsOfDate,shareWithResearchAsOfDate') \
STORED AS INPUTFORMAT \
  'org.apache.hadoop.mapred.TextInputFormat' \
OUTPUTFORMAT \
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
LOCATION\
  's3://mp-d609/customer/landing/'\
TBLPROPERTIES (\
  'CrawlerSchemaDeserializerVersion'='1.0', \
  'CrawlerSchemaSerializerVersion'='1.0', \
  'UPDATED_BY_CRAWLER'='custom_crawler', \
  'averageRecordSize'='307', \
  'classification'='json', \
  'compressionType'='none', \
  'objectCount'='1', \
  'recordCount'='956', \
  'sizeKey'='293777', \
  'typeOfData'='file')\
}