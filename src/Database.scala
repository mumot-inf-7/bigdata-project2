spark.sql("drop table if exists places")
spark.sql("""CREATE TABLE IF NOT EXISTS `places` (
 `id` long,
 `neighbourhood` string,
 `neighbourhood_group` string,
 `city` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("drop table if exists times")
spark.sql("""CREATE TABLE IF NOT EXISTS `times` (
 `date` string,
 `day` int,
 `month` int,
 `year` int)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("drop table if exists hosts")
spark.sql("""CREATE TABLE IF NOT EXISTS `hosts` (
 `id` long,
 `name` string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("drop table if exists grades")
spark.sql("""CREATE TABLE IF NOT EXISTS `grades` (
 `id` long,
 `grade` int)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("drop table if exists facts")
spark.sql("""CREATE TABLE IF NOT EXISTS `facts` (
 `place_id` long,
 `date` string,
 `host_id` long,
 `grade_id` long,
 `grade_sum` int,
 `price_sum` int,
 `available_flats_count` int,
 `flats_count` int)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")