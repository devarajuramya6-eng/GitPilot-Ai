from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="GitPilot AI API",
    description="Intelligent GitHub Issue Triage and Developer Matching Platform",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Welcome to GitPilot AI API"}

@app.get("/health")
async def health_check():
    return {"status": "ok", "version": "1.0.0"}
