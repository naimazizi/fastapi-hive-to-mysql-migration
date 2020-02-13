from fastapi import APIRouter, BackgroundTasks
from models.job_submission import JobSubmission
import logging
from starlette.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST
from starlette.responses import Response
import uuid
import os

from services import database, hive_operation, queue

router = APIRouter()

logging.getLogger("job")

def task(id:str, db_table:str, hive_table:str, file_path:str):
    status_code = 1

    if file_path is not None:
        _ = database.load_into_db(db_table, file_path, id)

    else:
        if hive_table is not None and file_path is None:
            file_path, status_code = hive_operation.generate_tsv(hive_table, id)
            if status_code == 0:
                _ = database.load_into_db(db_table, file_path, id)
                os.remove(file_path)
    queue.remove_queue(db_table)


@router.post("/migrate/", tags=["job"], status_code=HTTP_201_CREATED)
async def submit_job(job:JobSubmission, response:Response, background_task:BackgroundTasks):
    id = uuid.uuid4()
    logging.info("Got request id:{} to submit data migration job {}".format(id, str(job)))

    db_table = job.db_table_name
    hive_table = job.hive_table_name
    file_path = job.file_path

    if(queue.in_queue(db_table)):
        response.status_code = HTTP_400_BAD_REQUEST
        return "Task is Exists"
    else:
        if(file_path is not None and file_path != ''):
            if len(hive_table) > 0:
                response.status_code = HTTP_400_BAD_REQUEST
                return "Task is Rejected"
            else:
                queue.add_queue(id,db_table)
                background_task.add_task(task,id,db_table,None,file_path)
        else:
            if hive_table is None or hive_table == '':
                response.status_code = HTTP_400_BAD_REQUEST
                return "Task is Rejected"
            else:
                queue.add_queue(id,db_table)
                background_task.add_task(task,id,db_table,hive_table,None)
        return "Task Created"

@router.get("/migrate/", tags=["job"])
async def get_job():
    return queue.list_all()