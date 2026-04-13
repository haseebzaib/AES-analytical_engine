from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from analytics_engine.runtime import AnalyticsRuntime
from webpage.app import configure_webpage


runtime = AnalyticsRuntime()


@asynccontextmanager
async def lifespan(app: FastAPI):
    runtime.start()
    app.state.runtime = runtime
    try:
        yield
    finally:
        runtime.stop()


app = FastAPI(title="MetaCrust Edge Gateway", lifespan=lifespan)
configure_webpage(app)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
