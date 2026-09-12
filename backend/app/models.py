from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from .database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    repositories = relationship("Repository", back_populates="owner")

class Repository(Base):
    __tablename__ = "repositories"

    id = Column(Integer, primary_key=True, index=True)
    github_id = Column(Integer, unique=True, index=True)
    name = Column(String, index=True)
    full_name = Column(String, unique=True)
    owner_id = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.utcnow)

    owner = relationship("User", back_populates="repositories")
    issues = relationship("Issue", back_populates="repository")

class Issue(Base):
    __tablename__ = "issues"

    id = Column(Integer, primary_key=True, index=True)
    github_issue_id = Column(Integer, unique=True, index=True)
    repository_id = Column(Integer, ForeignKey("repositories.id"))
    title = Column(String)
    body = Column(String)
    state = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    repository = relationship("Repository", back_populates="issues")
