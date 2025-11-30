import pendulum
import pandas as pd
import uuid
import logging
from datetime import datetime, timedelta

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
def configure_task_logger(task_id: str) -> logging.Logger:
    """
    Configures and returns a logger for a specific Airflow task.

    Parameters:
        task_id (str): The ID of the Airflow task.
    """

    logger = logging.getLogger(task_id)
    logger.setLevel(logging.DEBUG)

    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger