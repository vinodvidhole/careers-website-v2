from sqlalchemy import create_engine
import pymysql
import os

dn_connection_string = os.environ['DB_CONNECTION_STRING']

engine = create_engine(
    dn_connection_string,
    connect_args={
        "ssl": {
            "ssl_ca": "ca.pem",
            "ssl_cert": "client-cert.pem",
            "ssl_key": "client-key.pem"
        }
    }
)

def load_jobs_from_db():
  stmt = "select * from jobs;"
  jobs = []
  with engine.connect() as conn:
      for row in conn.execute(stmt):
          jobs.append(dict(row))
      return jobs  
    

