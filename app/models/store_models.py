import uuid

from sqlalchemy import Column, Integer, Time, ForeignKey, CheckConstraint,String,TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base

class TimeZone(Base):
    __tablename__ = "timezone"

    store_id = Column(UUID, primary_key=True, index=True,unique = True)
    timezone_str = Column(String, default="America/Chicago")

    menu_hours = relationship("MenuHours", back_populates="menu", cascade="all, delete-orphan")
    statuses = relationship("StoreStatus", back_populates="store", cascade="all, delete-orphan")


class MenuHours(Base):
    __tablename__ = "menu_hours"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID, ForeignKey("timezone.store_id"), nullable=False)
    day_of_week = Column(Integer, CheckConstraint("day_of_week BETWEEN 0 AND 6"), nullable=False) # 0=Monday, 6=Sunday
    start_time_local = Column(Time, nullable=True)  
    end_time_local = Column(Time, nullable=True)  

    menu = relationship("TimeZone", back_populates="menu_hours")


class StoreStatus(Base):
    __tablename__ = "store_status"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    store_id = Column(UUID, ForeignKey("timezone.store_id"), nullable=False)
    status = Column(String, CheckConstraint("status IN ('active','inactive')"), nullable=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)

    store = relationship("TimeZone", back_populates="statuses")


