from pydantic import BaseModel, field_validator
from typing import List, Optional
from datetime import datetime, time
from uuid import UUID

class StoreStatusBase(BaseModel):
    timestamp_utc: datetime
    status: str

    @field_validator("status")
    def status_must_be_active_or_inactive(cls, stat):
        if stat not in ("active", "inactive"):
            raise ValueError("status must be 'active' or 'inactive'")
        return stat

class StoreStatusCreate(StoreStatusBase):
    pass

class StoreStatusRead(StoreStatusBase):
    id: UUID
    store_id: UUID

    class Config:
        orm_mode = True


class MenuHoursBase(BaseModel):
    day_of_week: int
    start_time_local: Optional[time] = None
    end_time_local: Optional[time] = None

class MenuHoursCreate(MenuHoursBase):
    pass

class MenuHoursRead(MenuHoursBase):
    id: UUID
    store_id: UUID

    class Config:
        orm_mode = True


class TimeZoneBase(BaseModel):
    timezone_str: Optional[str] = "America/Chicago"

class TimeZoneCreate(TimeZoneBase):
    pass

class TimeZoneRead(TimeZoneBase):
    store_id: UUID
    menu_hours: List[MenuHoursRead] = []
    statuses: List[StoreStatusRead] = []

    class Config:
        orm_mode = True
