from pydantic import BaseModel, Field, ConfigDict, EmailStr
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class IssueStatusSchema(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    IN_PROGRESS = "in_progress"

class IssueBase(BaseModel):
    title: str = Field(..., description="The title of the issue", max_length=255)
    description: Optional[str] = Field(None, description="Detailed description of the issue")
    status: IssueStatusSchema = Field(default=IssueStatusSchema.OPEN)
    repository_id: int

class IssueCreate(IssueBase):
    pass

class IssueUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[IssueStatusSchema] = None

class IssueResponse(IssueBase):
    id: int
    created_at: datetime
    updated_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

class DeveloperProfileBase(BaseModel):
    username: str = Field(..., max_length=100)
    email: EmailStr
    github_id: Optional[str] = None

class DeveloperProfileCreate(DeveloperProfileBase):
    pass

class DeveloperProfileResponse(DeveloperProfileBase):
    id: int
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

class DeveloperSkillBase(BaseModel):
    skill_name: str
    proficiency: float = Field(default=0.0, ge=0.0, le=10.0)

class DeveloperSkillResponse(DeveloperSkillBase):
    id: int
    developer_id: int
    
    model_config = ConfigDict(from_attributes=True)

class AssignmentRecommendationBase(BaseModel):
    issue_id: int
    developer_id: int
    match_score: float
    reasoning: Optional[str] = None

class AssignmentRecommendationResponse(AssignmentRecommendationBase):
    id: int
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    size: int

# Additional schemas for all models...
