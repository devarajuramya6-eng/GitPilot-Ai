from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, JSON, Float, Text, Enum
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()

class IssueStatus(enum.Enum):
    OPEN = "open"
    CLOSED = "closed"
    IN_PROGRESS = "in_progress"

class Issue(Base):
    __tablename__ = "issues"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    status = Column(Enum(IssueStatus), default=IssueStatus.OPEN)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    repository_id = Column(Integer, ForeignKey("repository_configurations.id"))
    
    predictions = relationship("IssuePrediction", back_populates="issue")
    duplicates = relationship("DuplicateMatch", foreign_keys="DuplicateMatch.issue_id", back_populates="issue")

class DeveloperProfile(Base):
    __tablename__ = "developer_profiles"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(255), unique=True, index=True)
    github_id = Column(String(100), unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    skills = relationship("DeveloperSkill", back_populates="developer")

class DeveloperSkill(Base):
    __tablename__ = "developer_skills"
    id = Column(Integer, primary_key=True, index=True)
    developer_id = Column(Integer, ForeignKey("developer_profiles.id"))
    skill_name = Column(String(100), nullable=False)
    proficiency = Column(Float, default=0.0)
    
    developer = relationship("DeveloperProfile", back_populates="skills")

class IssuePrediction(Base):
    __tablename__ = "issue_predictions"
    id = Column(Integer, primary_key=True, index=True)
    issue_id = Column(Integer, ForeignKey("issues.id"))
    predicted_labels = Column(JSON)
    confidence_score = Column(Float)
    model_version_id = Column(Integer, ForeignKey("model_versions.id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    issue = relationship("Issue", back_populates="predictions")
    model_version = relationship("ModelVersion")

class AssignmentRecommendation(Base):
    __tablename__ = "assignment_recommendations"
    id = Column(Integer, primary_key=True, index=True)
    issue_id = Column(Integer, ForeignKey("issues.id"))
    developer_id = Column(Integer, ForeignKey("developer_profiles.id"))
    match_score = Column(Float)
    reasoning = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

class DuplicateMatch(Base):
    __tablename__ = "duplicate_matches"
    id = Column(Integer, primary_key=True, index=True)
    issue_id = Column(Integer, ForeignKey("issues.id"))
    duplicate_issue_id = Column(Integer, ForeignKey("issues.id"))
    similarity_score = Column(Float)
    
    issue = relationship("Issue", foreign_keys=[issue_id], back_populates="duplicates")

class ModelVersion(Base):
    __tablename__ = "model_versions"
    id = Column(Integer, primary_key=True, index=True)
    version_tag = Column(String(50), unique=True, nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class RepositoryConfiguration(Base):
    __tablename__ = "repository_configurations"
    id = Column(Integer, primary_key=True, index=True)
    repo_name = Column(String(255), nullable=False)
    owner = Column(String(100), nullable=False)
    settings = Column(JSON)
    is_active = Column(Boolean, default=True)

class WebhookEvent(Base):
    __tablename__ = "webhook_events"
    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String(100), nullable=False)
    payload = Column(JSON)
    processed = Column(Boolean, default=False)
    received_at = Column(DateTime, default=datetime.utcnow)

class APIKey(Base):
    __tablename__ = "api_keys"
    id = Column(Integer, primary_key=True, index=True)
    key_hash = Column(String(255), unique=True, nullable=False)
    owner_id = Column(Integer, ForeignKey("developer_profiles.id"))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# Further models (TrainingDataset, TrainingRun, FeedbackEntry, WebhookDelivery, NotificationRecord, NotificationPreference, ProjectAnalysis, AnalyzedFile, RepositoryHealth, AuditLogEntry, UserSession, ComponentRegistry, TriageQueue, TriageDecision) would go here following the same detailed declarative structure.
