import setting

import pymysql
import logging
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,Session

from typing import List,Any

from models.job_status import StatusSchema,Status

logging.getLogger("database")

SQLALCHEMY_DATABASE_URL = "sqlite:///{}".format(setting.SQLITE_DB_PATH)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)

def get_status(db: Session, skip:int = 0, limit:int = 100) -> List[Status]:
    return db.query(Status).offset(skip).limit(limit).all()

def get_status_by_id(db: Session, id:str) -> List[Status]:
    return db.query(Status).filter(Status.id == id).all()

def insert_status(db: Session, status: StatusSchema) -> Status:
    status_ = Status(id = status.id, db_table = status.db_table, status = status.status, created_date = status.created_date, updated_date = status.updated_date)
    db.add(status_)
    db.commit()
    db.refresh(status_)
    return status_

def load_into_db(table:str, file_path:str, id:str) -> int:
    return_status = 1

    db_conn = pymysql.connect(
        host=setting.DB_HOST, 
        user=setting.DB_USERNAME, 
        password=setting.DB_PASSWORD, 
        database=setting.DB_USERNAME, 
        charset='utf8mb4', 
        cursorclass=pymysql.cursors.DictCursor, 
        local_infile=True)

    logging.debug("Database connection is successfully created")

    query = '''LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '\t' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;'''.format(file_path, table)

    try:
        with db_conn.cursor() as cursor:
            cursor.execute(query)
        db_conn.commit()
        return_status = 0
        logging.info("Task id:{}. Succesfuly load {} into {}".format(id, file_path, table))
    except pymysql.MySQLError as e:
        logging.info('Task id:{}. Got error {!r}, errno is {}'.format(id, e, e.args[0]))
    finally:
        db_conn.close()
        logging.debug("Database connection is succesfully closed")

    return return_status