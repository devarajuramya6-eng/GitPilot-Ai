from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from . import crud, models, schemas, auth
from .database import get_db

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
    # Deterministic local ML inference simulation matching local engine specs
    return {
        "issue_id": issue_id,
        "classification": {"type": "Security", "confidence": 0.94},
        "priority": {"level": "Critical", "confidence": 0.91},
        "complexity": {"level": "Hard", "score": 88},
        "duplicate_detection": {"top_candidate_id": 109, "similarity": 0.84},
        "recommended_developer": {
            "name": "Anil K.",
            "match_score": 94,
            "workload_status": "Optimal"
        },
        "execution_time_ms": 18,
        "mode": "Local Machine Learning Pipeline (Zero External API Calls)"
    }

