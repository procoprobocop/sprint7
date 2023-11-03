import os
import sys
from datetime import datetime, timedelta
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SQLContext

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

def input_paths(date, depth, input_path):
    paths = []
    for i in range(0, depth, 1):
        pth = input_path + '/date=' + str(
            datetime.date(datetime.fromisoformat(date)) + timedelta(days=(-1) * i)) + '/event_type=message'
        paths.append(pth)
    return paths

def main():
    date = sys.argv[1]
    depth = int(sys.argv[2])
    cut = int(sys.argv[3])
    input_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    output_path = sys.argv[6] + '/date=' + date  # Добавляем дату в путь

    conf = SparkConf().setAppName(f"VerifiedTagsCandidatesJob-{date}-d{depth}-cut{cut}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    paths = input_paths(date, depth, input_path)
    messages = sql.read.parquet(*paths)

    tags_users = messages.filter(F.col('event.message_channel_to').isNotNull()).select(
        messages.event.message_from.alias("user_id"), F.explode(messages.event.tags).alias("tag"))

    tags_users.show(10)

    tags_col = tags_users.groupBy('tag') \
        .agg(F.countDistinct("user_id").alias("suggested_count")) \
        .orderBy(F.col('suggested_count').desc()) \
        .where(f"suggested_count >= {cut}")

    tags_col.show(10)

    verified_tags = sql.read.parquet(verified_tags_path)

    verified_tags.show(10)

    candidates = tags_col.join(verified_tags, "tag", "left_anti")

    candidates = candidates.select('tag', 'suggested_count')
    candidates.show(10)

    candidates.write.option("header", True) \
        .mode("overwrite") \
        .parquet(output_path)

if __name__ == "__main__":
    main()
