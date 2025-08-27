import enum
import uuid
from sqlalchemy import Column, String, Integer, DateTime, Enum, ForeignKey, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.db.session import Base


class ReportStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class Report(Base):
    __tablename__ = "reports"

    report_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status = Column(Enum(ReportStatus), default=ReportStatus.RUNNING, nullable=False)

    # One report → many results
    results = relationship("ReportResult", back_populates="report", cascade="all, delete-orphan")
    jobs = relationship("ReportJob", back_populates="report", cascade="all, delete-orphan")

class ReportResult(Base):
    __tablename__ = "report_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    report_id = Column(UUID(as_uuid=True), ForeignKey("reports.report_id"))
    store_id = Column(UUID(as_uuid=True), ForeignKey("timezone.store_id"))

    # Uptime
    uptime_last_hour_in_minutes = Column(Integer, default=0)
    uptime_last_day_in_hours = Column(Integer, default=0)
    uptime_last_week_in_hours = Column(Integer, default=0)

    # Downtime
    downtime_last_hour_in_minutes = Column(Integer, default=0)
    downtime_last_day_in_hours = Column(Integer, default=0)
    downtime_last_week_in_hours = Column(Integer, default=0)

    # Many results → one report
    report = relationship("Report", back_populates="results")


class ReportJob(Base):
    __tablename__ = "report_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)  # <- primary key
    report_id = Column(UUID(as_uuid=True), ForeignKey("reports.report_id"), nullable=False)
    status = Column(Enum(ReportStatus), nullable=False, default=ReportStatus.RUNNING)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    file_path = Column(String, nullable=True)

    report = relationship("Report", back_populates="jobs")

