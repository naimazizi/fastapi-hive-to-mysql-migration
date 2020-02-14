from fastapi import FastAPI
from routers import job
from services.database import engine
from models import job_status
import uvicorn

job_status.Base.metadata.create_all(bind=engine)

app = FastAPI()

app.include_router(
    job.router,
    prefix='/job',
    tags=['job'])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)