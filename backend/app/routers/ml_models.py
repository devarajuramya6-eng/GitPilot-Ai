import logging
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, BackgroundTasks, status
from pydantic import BaseModel, Field

router = APIRouter(prefix="/ml-models", tags=["ml-models"])
logger = logging.getLogger(__name__)

class ModelMetrics(BaseModel):
    accuracy: float = Field(..., description="Model accuracy on validation set")
    precision: float = Field(..., description="Precision score")
    recall: float = Field(..., description="Recall score")
    f1_score: float = Field(..., description="F1 score")
    loss: float = Field(..., description="Evaluation loss")

class ModelVersion(BaseModel):
    version_id: str
    model_name: str
    is_active: bool
    created_at: datetime
    metrics: ModelMetrics
    hyperparameters: Dict[str, Any]

class TrainingJobResponse(BaseModel):
    job_id: str
    status: str
    started_at: datetime
    model_name: str

class FeatureImportance(BaseModel):
    feature_name: str
    importance_score: float

@router.get("/", response_model=List[ModelVersion])
async def list_models():
    """List all deployed machine learning models and their current active versions."""
    return [
        ModelVersion(
            version_id="v1.4.2",
            model_name="issue_triage_classifier",
            is_active=True,
            created_at=datetime.now(timezone.utc),
            metrics=ModelMetrics(accuracy=0.92, precision=0.91, recall=0.93, f1_score=0.92, loss=0.21),
            hyperparameters={"learning_rate": 0.001, "batch_size": 64, "epochs": 50}
        )
    ]

@router.get("/{model_name}/versions", response_model=List[ModelVersion])
async def get_model_versions(model_name: str):
    """Retrieve the history of versions for a specific model."""
    if model_name != "issue_triage_classifier":
        raise HTTPException(status_code=404, detail="Model not found")
    return []

@router.post("/{model_name}/retrain", response_model=TrainingJobResponse)
async def trigger_model_retrain(model_name: str, background_tasks: BackgroundTasks):
    """Trigger a new retraining job for the specified model in the background."""
    job_id = f"job_{datetime.now(timezone.utc).timestamp()}"
    async def retrain_task():
        logger.info(f"Starting retraining for {model_name} (Job: {job_id})")
        await asyncio.sleep(2)
        logger.info(f"Finished retraining for {model_name}")
    
    background_tasks.add_task(retrain_task)
    return TrainingJobResponse(
        job_id=job_id,
        status="running",
        started_at=datetime.now(timezone.utc),
        model_name=model_name
    )

@router.get("/{model_name}/feature-importance", response_model=List[FeatureImportance])
async def get_feature_importances(model_name: str):
    """Get the feature importances for a trained model."""
    return [
        FeatureImportance(feature_name="title_length", importance_score=0.15),
        FeatureImportance(feature_name="body_word_count", importance_score=0.35),
        FeatureImportance(feature_name="is_bug_label", importance_score=0.25),
        FeatureImportance(feature_name="code_snippet_count", importance_score=0.25)
    ]

@router.post("/{model_name}/predict")
async def test_prediction(model_name: str, payload: Dict[str, Any]):
    """Test the prediction endpoint of the current active model version."""
    logger.info(f"Running inference on {model_name} with payload: {payload}")
    return {"prediction": "bug", "confidence": 0.94}
