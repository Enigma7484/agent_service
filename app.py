from fastapi import FastAPI, UploadFile, File

app = FastAPI()

@app.get("/")
def health():
    return {"ok": True}

@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    return {
        "filename": file.filename,
        "content_type": file.content_type
    }