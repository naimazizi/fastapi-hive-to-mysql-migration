import setting

import pymysql
import logging

logging.getLogger("database")

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

    query = '''LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;'''.format(file_path, table)

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
