from sqlalchemy.orm import Session
from . import models, schemas
from .auth import get_password_hash

def get_user_by_email(db: Session, email: str):
    return db.query(models.User).filter(models.User.email == email).first()

def create_user(db: Session, user: schemas.UserCreate):
    hashed_password = get_password_hash(user.password)
    db_user = models.User(email=user.email, hashed_password=hashed_password)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def get_repositories(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Repository).offset(skip).limit(limit).all()

def create_repository(db: Session, repository: schemas.RepositoryCreate, user_id: int):
    db_repo = models.Repository(**repository.model_dump(), owner_id=user_id)
    db.add(db_repo)
    db.commit()
    db.refresh(db_repo)
    return db_repo
