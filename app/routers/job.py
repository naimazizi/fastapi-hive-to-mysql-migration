from fastapi import APIRouter, BackgroundTasks, Depends
from models.job_submission import JobSubmission
import logging
from starlette.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST
from starlette.responses import Response
import uuid
import os
from sqlalchemy.orm import Session

from typing import List

from services import database, hive_operation, queue
from services.database import SessionLocal,insert_status
from models.job_status import StatusSchema
from utils.constants import ReturnCode

router = APIRouter()

logging.getLogger("job")

def task(id:str, db_table:str, hive_table:str, file_path:str, db:Session):
    status_code = 1

    if file_path is not None:
        status_code = database.load_into_db(db_table, file_path, id)
        
        if status_code != 0:
            status = StatusSchema(id = id, db_table = db_table, status = ReturnCode.ERROR_IN_DB)
            insert_status(db,status)


    else:
        if hive_table is not None and file_path is None:
            file_path, status_code = hive_operation.generate_tsv(hive_table, id)
            if status_code != 0:
                status = StatusSchema(id = id, db_table = db_table, status = ReturnCode.ERROR_IN_HIVE)
                insert_status(db,status)
            else:
                status_code = database.load_into_db(db_table, file_path, id)
                os.remove(file_path)

                if status_code != 0 :
                    status = StatusSchema(id = id, db_table = db_table, status = ReturnCode.ERROR_IN_DB)
                    insert_status(db,status)
    
    queue.remove_queue(db_table)
    status = StatusSchema(id = id, db_table = db_table, status = ReturnCode.SUCCESS)
    insert_status(db,status)

def get_db():
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()

@router.post("/migrate/", tags=["job"], status_code=HTTP_201_CREATED)
async def submit_job(job:JobSubmission, response:Response, background_task:BackgroundTasks, db: Session = Depends(get_db)):
    id = uuid.uuid4()
    logging.info("Got request id:{} to submit data migration job {}".format(id, str(job)))

    db_table = job.db_table_name
    hive_table = job.hive_table_name
    file_path = job.file_path

    if(queue.in_queue(db_table)):
        status = StatusSchema(id = id, db_table = db_table, status = ReturnCode.TASK_IS_EXISTS)
        insert_status(db,status)
        response.status_code = HTTP_400_BAD_REQUEST
        return "Task is Exists"
    else:
        if(file_path is not None and file_path != ''):
            if len(hive_table) > 0:
                status = StatusSchema(id = str(id), db_table = db_table, status = ReturnCode.BAD_REQUEST)
                insert_status(db,status)
                response.status_code = HTTP_400_BAD_REQUEST
                return "Task is Rejected"
            else:
                queue.add_queue(id,db_table)
                background_task.add_task(task,id,db_table,None,file_path,db)
        else:
            if hive_table is None or hive_table == '':
                status = StatusSchema(id = id, db_table = db_table, status = ReturnCode.BAD_REQUEST)
                insert_status(db,status)
                response.status_code = HTTP_400_BAD_REQUEST
                return "Task is Rejected"
            else:
                queue.add_queue(id,db_table)
                background_task.add_task(task,id,db_table,hive_table,None,db)
        return "Task Created"

@router.get("/migrate", tags=["job"], response_model=List[StatusSchema])
async def get_job(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return database.get_status(db,skip,limit)

@router.get("/migrate/", tags=["job"], response_model=List[StatusSchema])
async def get_job_by_id(id:str, db: Session = Depends(get_db)):
    return database.get_status_by_id(db,id)
