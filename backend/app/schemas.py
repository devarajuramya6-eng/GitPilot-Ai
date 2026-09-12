from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class UserBase(BaseModel):
    email: str

class UserCreate(UserBase):
    password: str

class User(UserBase):
    id: int
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True

class RepositoryBase(BaseModel):
    name: str
    full_name: str
    github_id: int

class RepositoryCreate(RepositoryBase):
    pass

class Repository(RepositoryBase):
    id: int
    owner_id: int
    created_at: datetime

    class Config:
        from_attributes = True

class IssueBase(BaseModel):
    title: str
    body: Optional[str] = None
    state: str
    github_issue_id: int

class IssueCreate(IssueBase):
    pass

class Issue(IssueBase):
    id: int
    repository_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
