from typing import Optional
from pydantic import BaseModel
from uuid import UUID

from sqlalchemy import Enum

class ReportBase(BaseModel):
    report_id: UUID
    status: str

class ReportStatus(Enum):
    """Report generation status enum"""
    RUNNING = "Running"
    COMPLETE = "Complete"
    FAILED = "Failed"
    
class ReportResultBase(BaseModel):
    store_id: UUID
    uptime_last_hour_in_minutes: int
    uptime_last_day_in_hours: float
    uptime_last_week_in_hours: float
    downtime_last_hour_in_minutes: int
    downtime_last_day_in_hours: float
    downtime_last_week_in_hours: float

class ReportResultResponse(BaseModel):
    report_id: UUID
    status: str
    file_path: str

