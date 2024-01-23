import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

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
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    pass

def create_spark_connection():
    try:
        spark_session = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,'
                                            'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        
        spark_session.sparkContext.setLogLevel('ERROR')
        logging.info('Spark connection created successfully')
        return spark_session

    except Exception as e:
        logging.error(f"Couldn't create spark session due to: {e}")
        return None

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Couldn't create cassandra connection due to: {e}")
        return None

if __name__ == "__main__":
    spark_connection = create_spark_connection()
    if spark_connection is not None:
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)
