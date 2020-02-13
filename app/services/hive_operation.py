import subprocess
from typing import Dict
import logging
import time

import setting

logging.getLogger('hive_operation')

def generate_tsv(hive_table:str, id:str) -> Dict[object,int]:
    return_code = 1
    file_path = None
    try:
        logging.info("Task id:{}. Start extracting hive table to tsv format of table: {}".format(id, hive_table))
        env = {'HADOOP_CLIENT_OPTS':'-Djline.terminal=jline.UnsupportedTerminal'}

        BEELINE_CMD='beeline ' \
                    '-u "jdbc:hive2://{}/;serviceDiscoveryMode=zooKeeper;hive.server2.proxy.user={}" ' \
                    '--silent=true --outputformat=tsv2'.format(setting.HIVE_URL, setting.PROXY_USER)

        file_path = "{}/{}.tsv".format(setting.TEMP_PATH,hive_table)

        process = subprocess.run(
                        '{0} -f "select * from {1}" > {}'.format(BEELINE_CMD, hive_table, file_path),
                        shell=True, env=env
                    )

        return_code = int(process.returncode)
        if return_code == 0:
            logging.info("Task id:{}. Succesfully extracted hive table of table: {}".format(id, hive_table))
        else:
            logging.error(
                    "Task id:{}. Failed to generate tsv file of table:{}".format(
                        id, hive_table
                    ))

    except Exception as e:
        logging.error(
                    "Task id:{}. Failed to generate tsv file of table:{}".format(
                        id, hive_table
                    ))

    return file_path, return_code