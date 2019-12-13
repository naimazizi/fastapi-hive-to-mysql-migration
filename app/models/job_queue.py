from pydantic import BaseModel
from utils import utility_function
from datetime import datetime

class JobQueue(BaseModel):
    id:str
    db_table:str = None
    status:str = None
    created_date:str = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
    updated_date:str = None