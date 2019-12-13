from typing import Dict
from models.job_submission import JobSubmission

WORKING_QUEUE:Dict[str,str] = dict()

def add_queue(id:str, db_table:str) -> None:
    WORKING_QUEUE[db_table] = id

def in_queue(db_table:str) -> bool:
    value = WORKING_QUEUE.get(db_table)
    if value is None:
        return False
    else:
        return True

def remove_queue(db_table:str) -> None:
    WORKING_QUEUE.pop(db_table)

def list() -> Dict[str,str]:
    return WORKING_QUEUE
