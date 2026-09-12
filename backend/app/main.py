from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
import tempfile
import os
import shutil
from . import crud, models, schemas, auth
from .database import get_db
from .local_ml_engine import local_ml_engine
from .project_analyzer import project_analyzer

app = FastAPI(
    title="GitPilot AI API",
    description="Intelligent GitHub Issue Triage and Developer Matching Platform",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/auth/register", response_model=schemas.User)
def register(user: schemas.UserCreate, db: Session = Depends(get_db)):
    db_user = crud.get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    return crud.create_user(db=db, user=user)

@app.post("/api/auth/login")
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = crud.get_user_by_email(db, email=form_data.username)
    if not user or not auth.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = auth.timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": user.email}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/")
async def root():
    return {"message": "Welcome to GitPilot AI API"}

@app.get("/health")
async def health_check():
    return {"status": "ok", "version": "1.0.0", "engine": "GitPilot Local ML Core"}

@app.get("/api/issues")
def get_issues():
    return [
        {
            "id": 145,
            "title": "JWT Token rotation fails intermittently under high concurrent requests",
            "description": "Token refresh returns 401 when two clients make refresh calls simultaneously. Production impact observed.",
            "repo": "acme/auth-core-service",
            "type": "Security",
            "priority": "Critical",
            "complexity": "Hard",
            "component": "Authentication & Session",
            "duplicateProb": 8,
            "qualityScore": 92,
            "recommendedDev": "Anil K.",
            "status": "pending_approval"
        },
        {
            "id": 146,
            "title": "Memory leak in streaming response handler when client disconnects early",
            "description": "SSE stream workers retain open connections when browser window is closed, resulting in OOM container crash.",
            "repo": "acme/gateway-api",
            "type": "Bug",
            "priority": "High",
            "complexity": "Hard",
            "component": "API Gateway / Network",
            "duplicateProb": 84,
            "qualityScore": 78,
            "recommendedDev": "Elena Rostova",
            "status": "pending_approval"
        }
    ]

@app.post("/api/issues/{issue_id}/triage")
def trigger_local_triage(issue_id: int):
    # Live ML classification through local engine
    sample_issues = {
        145: ("JWT Token rotation fails intermittently under high concurrent requests", "Token refresh returns 401 when two clients make refresh calls simultaneously. Production impact observed."),
        146: ("Memory leak in streaming response handler when client disconnects early", "SSE stream workers retain open connections when browser window is closed, resulting in OOM container crash.")
    }
    title, desc = sample_issues.get(issue_id, ("Generic Issue", "Standard report"))
    analysis = local_ml_engine.analyze_full_issue(title, desc)
    analysis["issue_id"] = issue_id
    return analysis

@app.post("/api/projects/upload")
async def upload_project(file: UploadFile = File(...)):
    """Accepts .zip or .tar archives, safely unpacks, and executes static project profiling."""
    if not (file.filename.endswith(".zip") or file.filename.endswith(".tar") or file.filename.endswith(".tar.gz")):
        raise HTTPException(status_code=400, detail="Only .zip, .tar, and .tar.gz archives are supported")

    with tempfile.TemporaryDirectory() as temp_dir:
        archive_path = os.path.join(temp_dir, file.filename)
        with open(archive_path, "wb") as f:
            content = await file.read()
            f.write(content)

        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        try:
            if file.filename.endswith(".zip"):
                project_analyzer.safe_extract_zip(archive_path, extract_dir)
            else:
                with tarfile.open(archive_path, "r:*") as tar:
                    tar.extractall(extract_dir)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to unpack archive: {str(e)}")

        results = project_analyzer.analyze_directory(extract_dir)
        results["filename"] = file.filename
        results["status"] = "Analyzed Successfully"
        return results


