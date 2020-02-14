from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime
from datetime import datetime

Base = declarative_base()

class Status(Base):
    __tablename__ = "status"
    
    id = Column(String, primary_key=True, index=True)
    db_table = Column(String, index=True)
    status = Column(String, index=True)
    created_date = Column(DateTime, default=datetime.now())
    updated_date = Column(DateTime)

class StatusSchema(BaseModel):
    id:str
    db_table:str
    status:str
    created_date:datetime = datetime.now()
    updated_date:datetime = datetime.now()
    class Config:
        orm_mode = True