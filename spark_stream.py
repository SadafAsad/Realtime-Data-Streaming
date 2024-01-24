from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import logging

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS user_stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("--- Keyspace Created Successfully ---")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS user_stream.users_info (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        birthdate TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("--- Table Created Successfully ---")

# def insert_data(session, **kwargs):
#     user_id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     birthdate = kwargs.get('dob')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')

#     try:
#         session.execute("""
#             INSERT INTO user_stream.users_info(id, first_name, last_name, birthdate, gender, address, 
#                 post_code, email, username, dob, registered_date, phone, picture)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (user_id, first_name, last_name, birthdate, gender, address,
#               postcode, email, username, dob, registered_date, phone, picture))
#         logging.info(f"Data inserted for {user_id} {username}")

#     except Exception as e:
#         logging.error(f'FAILED to insert data: {e}')

def create_spark_connection():
    try:
        spark_session = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,'
                                            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        
        spark_session.sparkContext.setLogLevel('ERROR')
        logging.info('--- Spark Connection Created Successfully ---')
        return spark_session

    except Exception as e:
        logging.error(f"FAILED to create Spark session: {e}")
        return None

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])
        return cluster.connect()
    except Exception as e:
        logging.error(f"FAILED to create Cassandra connection: {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_info') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("--- Kafka Dataframe Created Successfully ---")
        return spark_df
    except Exception as e:
        logging.warning(f"FAILED to create Kafka dataframe: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("birthdate", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

if __name__ == "__main__":
    spark_connection = create_spark_connection()
    if spark_connection is not None:
        spark_df = connect_to_kafka(spark_connection)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)

            session = create_cassandra_connection()
            if session is not None:
                create_keyspace(session)
                create_table(session)
                # insert_data(session)

                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'user_stream')
                                .option('table', 'users_info')
                                .start())

                streaming_query.awaitTermination()
