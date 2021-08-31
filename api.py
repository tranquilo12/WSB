from fastapi import FastAPI

app = FastAPI()


@app.get("/wsb")
async def root():
    return {"message": "Hello World"}
