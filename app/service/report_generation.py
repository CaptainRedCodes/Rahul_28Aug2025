from collections import defaultdict
from uuid import UUID
import csv
from datetime import datetime, timedelta, time
from io import StringIO
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from zoneinfo import ZoneInfo

from app.db.session import SessionLocal
from app.models.report_models import Report, ReportJob, ReportResult, ReportStatus
from app.models.store_models import StoreStatus, TimeZone, MenuHours

DEFAULT_TIMEZONE = "America/Chicago"
REPORT_STORAGE_PATH = Path("reports")
REPORT_STORAGE_PATH.mkdir(exist_ok=True)


def _get_reference_data(session: Session) -> Tuple[Dict[UUID, str], Dict[Tuple[UUID, int], Tuple[time, time]]]:
    """Get timezone and business hours lookups in single queries"""
    # Get timezone lookup
    tz_query = "SELECT store_id, timezone_str FROM timezones"
    tz_result = session.execute(text(tz_query)).fetchall()
    tz_lookup = {UUID(str(row[0])): row[1] or DEFAULT_TIMEZONE for row in tz_result}
    
    # Get business hours lookup
    bh_query = """
    SELECT store_id, day_of_week, start_time_local, end_time_local 
    FROM menu_hours 
    WHERE start_time_local IS NOT NULL AND end_time_local IS NOT NULL
    """
    bh_result = session.execute(text(bh_query)).fetchall()
    bh_lookup = {}
    for row in bh_result:
        store_id = UUID(str(row[0]))
        day = int(row[1])
        start_time = row[2] if isinstance(row[2], time) else datetime.strptime(str(row[2]), '%H:%M:%S').time()
        end_time = row[3] if isinstance(row[3], time) else datetime.strptime(str(row[3]), '%H:%M:%S').time()
        bh_lookup[(store_id, day)] = (start_time, end_time)
    
    return tz_lookup, bh_lookup


def _get_business_hours(bh_lookup: Dict, store_id: UUID, day_of_week: int) -> Tuple[Optional[time], Optional[time]]:
    """Get business hours for store on specific day"""
    return bh_lookup.get((store_id, day_of_week), (None, None))


def _convert_to_local_timezone(utc_timestamp: datetime, timezone_str: str) -> datetime:
    """Convert UTC timestamp to local timezone"""
    utc_tz = ZoneInfo("UTC")
    local_tz = ZoneInfo(timezone_str)
    
    if utc_timestamp.tzinfo is None:
        utc_timestamp = utc_timestamp.replace(tzinfo=utc_tz)
    else:
        utc_timestamp = utc_timestamp.astimezone(utc_tz)
    
    return utc_timestamp.astimezone(local_tz)


def _calculate_period_uptime(logs: List[Tuple[datetime, str]], period_start: datetime, period_end: datetime) -> Tuple[float, float]:
    """Calculate uptime/downtime for a period using interpolation"""
    total_minutes = (period_end - period_start).total_seconds() / 60
    
    if not logs:
        return total_minutes, 0.0  # Assume uptime if no data
    
    # Filter logs to period
    period_logs = [(ts, status) for ts, status in logs if period_start <= ts <= period_end]
    if not period_logs:
        return total_minutes, 0.0
    
    period_logs.sort(key=lambda x: x[0])
    
    uptime = 0.0
    current_time = period_start
    current_status = period_logs[0][1]  # Extrapolate backwards from first log
    
    # Add time from period start to first log
    first_time = period_logs[0][0]
    if first_time > period_start:
        duration = (first_time - period_start).total_seconds() / 60
        uptime += duration if current_status == 'active' else 0
        current_time = first_time
    
    # Process each log transition
    for timestamp, status in period_logs:
        if timestamp > current_time:
            duration = (timestamp - current_time).total_seconds() / 60
            uptime += duration if current_status == 'active' else 0
        current_time = timestamp
        current_status = status
    
    # Extrapolate from last log to period end
    if current_time < period_end:
        duration = (period_end - current_time).total_seconds() / 60
        uptime += duration if current_status == 'active' else 0
    
    downtime = total_minutes - uptime
    return max(0, uptime), max(0, downtime)


def _calculate_store_metrics(store_logs: List[Tuple[datetime, str]], bh_lookup: Dict, 
                           store_id: UUID, max_timestamp: datetime) -> ReportResult:
    """Calculate all metrics for a store"""
    intervals = [
        (max_timestamp - timedelta(hours=1), max_timestamp),
        (max_timestamp - timedelta(days=1), max_timestamp),  
        (max_timestamp - timedelta(days=7), max_timestamp)
    ]
    
    metrics = []
    for start_time, end_time in intervals:
        total_uptime = 0.0
        total_downtime = 0.0
        
        # Process each day in the interval
        current_date = start_time.date()
        while current_date <= end_time.date():
            day_start = datetime.combine(current_date, time.min).replace(tzinfo=start_time.tzinfo)
            day_end = datetime.combine(current_date, time.max).replace(tzinfo=start_time.tzinfo)
            
            # Clip to interval bounds
            day_start = max(day_start, start_time)
            day_end = min(day_end, end_time)
            
            if day_start >= day_end:
                current_date += timedelta(days=1)
                continue
            
            # Get business hours
            day_of_week = current_date.weekday()
            bh_start, bh_end = _get_business_hours(bh_lookup, store_id, day_of_week)
            
            if bh_start is None:  # 24x7 operation
                business_start = day_start
                business_end = day_end
            else:
                business_start = datetime.combine(current_date, bh_start).replace(tzinfo=start_time.tzinfo)
                business_end = datetime.combine(current_date, bh_end).replace(tzinfo=start_time.tzinfo)
                
                if bh_end < bh_start:  # Overnight hours
                    business_end += timedelta(days=1)
                
                business_start = max(business_start, day_start)
                business_end = min(business_end, day_end)
            
            if business_start < business_end:
                day_uptime, day_downtime = _calculate_period_uptime(store_logs, business_start, business_end)
                total_uptime += day_uptime
                total_downtime += day_downtime
            
            current_date += timedelta(days=1)
        
        metrics.append((total_uptime, total_downtime))
    
    # Convert to required units
    hour_up, hour_down = metrics[0]
    day_up, day_down = metrics[1] 
    week_up, week_down = metrics[2]
    
    return ReportResult(
        store_id=str(store_id),
        uptime_last_hour_in_minutes=round(hour_up),
        uptime_last_day_in_hours=round(day_up / 60, 2),
        uptime_last_week_in_hours=round(week_up / 60, 2),
        downtime_last_hour_in_minutes=round(hour_down),
        downtime_last_day_in_hours=round(day_down / 60, 2),
        downtime_last_week_in_hours=round(week_down / 60, 2)
    )


def generate_report_csv(results: List[ReportResult]) -> str:
    """Generate CSV from results"""
    if not results:
        return ""
    
    output = StringIO()
    fieldnames = [
        'store_id',
        'uptime_last_hour(in minutes)',
        'uptime_last_day(in hours)', 
        'uptime_last_week(in hours)',
        'downtime_last_hour(in minutes)',
        'downtime_last_day(in hours)',
        'downtime_last_week(in hours)'
    ]
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for result in results:
        writer.writerow({
            'store_id': result.store_id,
            'uptime_last_hour(in minutes)': result.uptime_last_hour_in_minutes,
            'uptime_last_day(in hours)': result.uptime_last_day_in_hours,
            'uptime_last_week(in hours)': result.uptime_last_week_in_hours,
            'downtime_last_hour(in minutes)': result.downtime_last_hour_in_minutes,
            'downtime_last_day(in hours)': result.downtime_last_day_in_hours,
            'downtime_last_week(in hours)': result.downtime_last_week_in_hours
        })
    
    return output.getvalue()


def generate_full_report(report_id: UUID) -> None:
    """Generate complete uptime/downtime report"""
    db = SessionLocal()
    try:
        # Get reference data
        tz_lookup, bh_lookup = _get_reference_data(db)
        
        # Get max timestamp
        max_ts_result = db.execute(text("SELECT MAX(timestamp_utc) FROM store_status")).fetchone()
        if not max_ts_result[0]:
            raise ValueError("No status logs found")
        
        max_timestamp_utc = max_ts_result[0]
        if max_timestamp_utc.tzinfo is None:
            max_timestamp_utc = max_timestamp_utc.replace(tzinfo=ZoneInfo("UTC"))
        
        # Process stores using SQL to avoid loading all data
        results = []
        stores_query = "SELECT DISTINCT store_id FROM store_status"
        store_ids = [UUID(str(row[0])) for row in db.execute(text(stores_query)).fetchall()]
        
        for store_id in store_ids:
            try:
                # Get logs for this store only
                logs_query = text("""
                SELECT timestamp_utc, status 
                FROM store_status 
                WHERE store_id = :store_id 
                ORDER BY timestamp_utc
                """)
                
                raw_logs = db.execute(logs_query, {"store_id": str(store_id)}).fetchall()
                
                # Convert to local timezone
                timezone_str = tz_lookup.get(store_id, DEFAULT_TIMEZONE)
                local_logs = []
                for ts, status in raw_logs:
                    local_ts = _convert_to_local_timezone(ts, timezone_str)
                    local_logs.append((local_ts, status))
                
                # Convert max timestamp to store's local time
                local_max_timestamp = _convert_to_local_timezone(max_timestamp_utc, timezone_str)
                
                # Calculate metrics
                result = _calculate_store_metrics(local_logs, bh_lookup, store_id, local_max_timestamp)
                results.append(result)
                
            except Exception:
                continue  # Skip problematic stores
        
        # Generate CSV and save
        csv_content = generate_report_csv(results)
        report_file_path = REPORT_STORAGE_PATH / f"{report_id}.csv"
        
        with open(report_file_path, 'w', newline='', encoding='utf-8') as f:
            f.write(csv_content)
        
        # Update database
        job = db.query(ReportJob).filter(ReportJob.report_id == report_id).first()
        report = db.query(Report).filter(Report.report_id == report_id).first()
        
        if job:
            job.status = ReportStatus.COMPLETE
            job.file_path = str(report_file_path)
        if report:
            report.status = ReportStatus.COMPLETE
            
        db.commit()
        
    except Exception as e:
        job = db.query(ReportJob).filter(ReportJob.report_id == report_id).first()
        report = db.query(Report).filter(Report.report_id == report_id).first()
        
        if job:
            job.status = ReportStatus.FAILED
        if report:
            report.status = ReportStatus.FAILED
            
        db.commit()
        raise
    finally:
        db.close()


def create_report_job(session: Session, report_id: UUID) -> ReportJob:
    """Create a new report job"""
    job = ReportJob(report_id=report_id, status=ReportStatus.RUNNING)
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def update_report_job_status(session: Session, report_id: UUID, status: ReportStatus, file_path: Optional[str] = None) -> None:
    """Update report job status"""
    job = session.query(ReportJob).filter(ReportJob.report_id == report_id).first()
    if job:
        job.status = status.value
        if file_path:
            job.file_path = file_path
        job.updated_at = func.now()
        session.commit()


def get_report_job(session: Session, report_id: UUID) -> Optional[ReportJob]:
    """Get report job"""
    return session.query(ReportJob).filter(ReportJob.report_id == report_id).first()