import psycopg2
from sqlalchemy import create_engine

engine = create_enigne("")



conn = psycopg2.connect(database="dvdrental",
                        host="pg-db",
                        user="postgres",
                        password="P@ssw0rd",
                        port="5432")
cursor = conn.cursor()