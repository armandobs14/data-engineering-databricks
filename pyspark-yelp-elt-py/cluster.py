# import libraries
from os.path import abspath
from pyspark.sql import SparkSession

# main
if __name__ == '__main__':

    # init spark session
    spark = SparkSession \
            .builder \
            .appName("etl-yelp-py") \
            .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
            .enableHiveSupport() \
            .getOrCreate()
            
    # set log level
    spark.sparkContext.setLogLevel("INFO")

    # read business json
    df_business = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json("wasb://owshq-spark-hdinsight-cluster@owshqhdistorage.blob.core.windows.net/data/yelp_academic_dataset_business_*.json")

    # read user json
    df_user = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json("wasb://owshq-spark-hdinsight-cluster@owshqhdistorage.blob.core.windows.net/data/yelp_academic_dataset_user_*.json")

    # read review json
    df_review = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json("wasb://owshq-spark-hdinsight-cluster@owshqhdistorage.blob.core.windows.net/data/yelp_academic_dataset_review_*.json")

    # print schema
    df_business.printSchema()
    df_user.printSchema()
    df_review.printSchema()

    # display
    df_business.show()
    df_user.show()
    df_review.show()

    # register df into sql engine
    df_business.createOrReplaceTempView("business")
    df_user.createOrReplaceTempView("user")
    df_review.createOrReplaceTempView("review")

    # join into a new [df]
    df_join = spark.sql("""
        SELECT u.user_id,
               u.name AS user,
               u.average_stars AS user_avg_stars,
               u.useful AS user_useful,
               u.review_count AS user_review_count,
               u.yelping_since,
               b.business_id,
               b.name AS business,
               b.city,
               b.state,
               b.stars AS business_stars,
               b.review_count AS business_review_count,
               r.useful AS review_useful,
               r.stars AS review_stars,
               r.date AS date
        FROM review AS r
        INNER JOIN business AS b
        ON r.business_id = b.business_id
        INNER JOIN user AS u
        ON u.user_id = r.user_id
    """)

    # show df
    df_join.show()

    # stop spark session
    spark.stop()
