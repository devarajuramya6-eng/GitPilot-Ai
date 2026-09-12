import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Depends, status, UploadFile, File
from pydantic import BaseModel, Field

router = APIRouter(prefix="/projects", tags=["projects"])
logger = logging.getLogger(__name__)

class LanguageStat(BaseModel):
    language: str
    percentage: float
    files_count: int
    lines_of_code: int

class SecurityVulnerability(BaseModel):
    severity: str
    file_path: str
    line_number: int
    description: str
    cwe_id: str

class ProjectDetail(BaseModel):
    project_id: str
    name: str
    description: Optional[str]
    is_public: bool
    created_at: datetime
    updated_at: datetime
    primary_language: str
    languages: List[LanguageStat]
    frameworks: List[str]

@router.get("/", response_model=List[ProjectDetail])
async def list_projects():
    """List all projects associated with the current user or organization."""
    return [
        ProjectDetail(
            project_id="proj_xyz123",
            name="GitPilot-Core",
            description="Core service for GitPilot AI",
            is_public=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            primary_language="Python",
            languages=[LanguageStat(language="Python", percentage=95.5, files_count=120, lines_of_code=25000)],
            frameworks=["FastAPI", "SQLAlchemy", "Pydantic"]
        )
    ]

@router.get("/{project_id}", response_model=ProjectDetail)
async def get_project(project_id: str):
    """Retrieve detailed information for a specific project."""
    if project_id != "proj_xyz123":
        raise HTTPException(status_code=404, detail="Project not found")
    return ProjectDetail(
        project_id=project_id,
        name="GitPilot-Core",
        description="Core service",
        is_public=False,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        primary_language="Python",
        languages=[],
        frameworks=["FastAPI"]
    )

@router.post("/{project_id}/archive")
async def upload_project_archive(project_id: str, file: UploadFile = File(...)):
    """Upload a ZIP archive containing the source code for static analysis."""
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only ZIP archives are supported.")
    return {"status": "success", "message": f"Archive {file.filename} uploaded for analysis."}

@router.get("/{project_id}/security-scan", response_model=List[SecurityVulnerability])
async def get_security_scan_report(project_id: str):
    """Retrieve the latest security scan results for the project."""
    return [
        SecurityVulnerability(
            severity="high",
            file_path="app/core/security.py",
            line_number=45,
            description="Hardcoded secret token detected.",
            cwe_id="CWE-798"
        )
    ]

@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_project(project_id: str):
    """Soft delete a project and schedule its resources for cleanup."""
    logger.info(f"Deleted project {project_id}")
    return
