from pydantic import BaseModel

class JobSubmission(BaseModel):
    db_table_name:str
    hive_table_name:str = None
    file_path:str = None