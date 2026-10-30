
import logging
import uuid
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {
                'class' : 'SimpleStrategy', 
                'replication_factor' : '1'
            };
    """)

    print("Keyspace created sucessfully")




def create_table(session):
    session.execute(""" 
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    user_id = uuid.UUID(kwargs["id"])

    query = """
        INSERT INTO spark_streams.created_users (
            id, first_name, last_name, gender, address,
            post_code, email, username, dob,
            registered_date, phone, picture
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        user_id,
        kwargs["first_name"],
        kwargs["last_name"],
        kwargs["gender"],
        kwargs["address"],
        kwargs["post_code"],
        kwargs["email"],
        kwargs["username"],
        kwargs["dob"],
        kwargs["registered_date"],
        kwargs["phone"],
        kwargs["picture"],
    )

    try:
        session.execute(query, values)
        logging.info(
            "Inserted user %s %s (%s)",
            kwargs["first_name"],
            kwargs["last_name"],
            user_id,
        )
    except Exception:
        logging.exception("Cassandra insertion failed")
        raise


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
                .appName('SparkDataStreaming') \
                .config(
                    "spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
                ) \
                .config("spark.cassandra.connection.host", "localhost") \
                .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("spark connection created successfully ! ")
    
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e} ")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
                .format('kafka') \
                .option("kafka.bootstrap.servers", 'localhost:9092') \
                .option('subscribe', 'users_created') \
                .option('startingOffsets', 'earliest') \
                .load()
        logging.info("kafka dataframe created successfully")

    except Exception as e:
        logging.error(f"Could not connect to kafka because : {e}")


    return spark_df

   

def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        cas_session = cluster.connect()
        return cas_session

    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e} ")
        return None



def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(),False),
        StructField("first_name", StringType(),False),
        StructField("last_name", StringType(),False),
        StructField("gender", StringType(),False),
        StructField("address", StringType(),False),
        StructField("post_code", StringType(),False),
        StructField("email", StringType(),False),
        StructField("username", StringType(),False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(),False),
        StructField("phone", StringType(),False),
        StructField("picture", StringType(),False),

        ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
          .select(from_json(col('value'), schema).alias('data')).select("data.*")

    return sel
    
if __name__ == "__main__":

    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session is not None:
                create_keyspace(session)
                create_table(session)

                def write_to_cassandra_batch(df, epoch_id):
                    rows = df.collect()
                    for row in rows:
                        user_data = row.asDict()
                        insert_data(session, **user_data)

                logging.info("Streaming is being started ... ")
                
                streaming_query = (selection_df.writeStream
                                  .foreachBatch(write_to_cassandra_batch)
                                  .option('checkpointLocation', '/tmp/checkpoint-v2')
                                  .start())
                                  
                streaming_query.awaitTermination()
        else:
            logging.error("Pipeline stopped: Spark could not read from Kafka.")
