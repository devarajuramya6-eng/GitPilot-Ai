import logging
import logging.config
import json
import sys
from datetime import datetime
from typing import Any, Dict

class JSONFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after parsing the LogRecord.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
        }
        
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id
            
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_data)

def setup_logging(log_level: str = "INFO", enable_json: bool = True):
    """
    Configures standard python logging to output to stdout.
    """
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        
    console_handler = logging.StreamHandler(sys.stdout)
    
    if enable_json:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Third-party logger suppression
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    
    logger.info("Logging configured successfully", extra={"log_level": log_level, "json_format": enable_json})

class MLInferenceLogger:
    """
    Specialized logger for tracking ML predictions and outcomes.
    """
    def __init__(self, name: str = "ml.inference"):
        self.logger = logging.getLogger(name)
        
    def log_prediction(self, model_version: str, input_features: dict, prediction: Any, confidence: float, processing_time_ms: float):
        self.logger.info(
            "ML Prediction generated",
            extra={
                "ml_event": True,
                "model_version": model_version,
                "input_features": input_features,
                "prediction": prediction,
                "confidence": confidence,
                "processing_time_ms": processing_time_ms
            }
        )

# Initialize standard logging right away (could also be done explicitly at app startup)
# setup_logging()
