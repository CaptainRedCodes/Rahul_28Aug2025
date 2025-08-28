from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import FileResponse
from uuid import UUID
from typing import Dict
import logging
from pathlib import Path

from sqlalchemy.orm import Session
from app.models.report_models import Report, ReportJob, ReportStatus
from app.schema.report_schema import ReportResultResponse
from app.utils.load_data import load_csv
from app.db.session import SessionLocal, get_session
from app.service.report_generation import generate_full_report, get_report_job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Store Monitoring", version="2.4.2")


@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks,session: Session = Depends(get_session)) -> Dict[str, str]:
    """
    Trigger report generation.

    """
    try:
        new_report = Report(status=ReportStatus.RUNNING)
        session.add(new_report)
        session.commit()
        session.refresh(new_report)

        new_job = ReportJob(
            report_id=new_report.report_id,
            status=ReportStatus.RUNNING
        )
        session.add(new_job)
        session.commit()

        # background task for report generation
        background_tasks.add_task(generate_full_report, new_report.report_id)

        logger.info(f"Triggered report generation with report_id: {new_report.report_id}")

        return {"report_id": str(new_report.report_id)}

    except Exception as e:
        logger.error(f"Error triggering report: {e}")
        session.rollback()
        raise HTTPException(status_code=500, detail="Failed to trigger report generation")



@app.get("/get_report/{report_id}")
async def get_report(report_id: UUID, session: Session = Depends(get_session)) -> ReportResultResponse:
    """
    Get report status and data.
    """
    try:
        job = get_report_job(session, report_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Report not found")
        
        if job.status == ReportStatus.RUNNING:
            return ReportResultResponse(
                status=job.status.value, 
                report_id=str(report_id),
                file_path= str(None)
            )
        
        elif job.status == ReportStatus.COMPLETE:
            if not job.file_path:
                raise HTTPException(status_code=500, detail="Report file path not found")
            
            report_file_path = Path(job.file_path)
            
            if not report_file_path.exists():
                raise HTTPException(status_code=500, detail="Report file not found")
            
            try:
                return ReportResultResponse(
                    status=job.status.value,
                    report_id=report_id,
                    file_path = str(report_file_path)
                )
            
            except Exception as e:
                logger.error(f"Error reading report file {report_file_path}: {e}")
                raise HTTPException(status_code=500, detail="Error reading report file")
        
        elif job.status == ReportStatus.FAILED:
            raise HTTPException(status_code=500, detail="Report generation failed")
        
        else:
            raise HTTPException(status_code=500, detail="Unknown report status")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting report {report_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    

@app.get("/download_report/{report_id}")
async def download_report(report_id: UUID, session: Session = Depends(get_session)):
    """ Download the report in .csv file"""
    job = get_report_job(session, report_id)

    if not job or job.status != ReportStatus.COMPLETE:
        raise HTTPException(status_code=404, detail="Report not ready for download")

    report_file_path = Path(job.file_path)
    if not report_file_path.exists():
        raise HTTPException(status_code=500, detail="Report file not found")

    return FileResponse(
        path=report_file_path,
        media_type="text/csv",
        filename=f"report_{report_id}.csv"
    )

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/upload-data/")
def upload_data(db: Session = Depends(get_db)):
    """ Automatic upload of data of 3 files: menu_hours.csv,store_status.csv,timezones.csv"""
    load_csv(db)
    return {"status": "success"}