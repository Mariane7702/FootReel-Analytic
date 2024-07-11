
import time
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("FootReel Analytics")
         .config("spark.jars", "postgresql-42.7.3.jar")
         .getOrCreate()
         )

while True:
    df_goal = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "goals") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df_shoot = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "shots") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df_possession = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "possession") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # create stats of matches

    df_goal.createOrReplaceTempView("goal")
    df_shoot.createOrReplaceTempView("shoot")
    df_possession.createOrReplaceTempView("possession")

    df_goal = spark.sql("SELECT match_id, team, COUNT(*) as goals FROM goal GROUP BY match_id, team")
    df_shoot = spark.sql("SELECT match_id, team, COUNT(*) as shoots FROM shoot GROUP BY match_id, team")
    df_possession = spark.sql(
        "SELECT match_id, team, SUM(possession_time) as possession FROM possession GROUP BY match_id, team")

    # join the dataframes
    df = df_goal.join(df_shoot, ["match_id", "team"], "outer") \
        .join(df_possession, ["match_id", "team"], "outer") \
        .na.fill(0)

    df.show()

    # write to postgres
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "match_stats") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    time.sleep(10)





spark.stop()
