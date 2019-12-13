from fastapi import FastAPI
from routers import job
import uvicorn

app = FastAPI()

app.include_router(
    job.router,
    prefix='/job',
    tags=['job'])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)