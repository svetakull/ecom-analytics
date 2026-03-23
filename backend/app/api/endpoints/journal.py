"""Журнал операций — CRUD + загрузка банковских выписок."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.services.journal_service import (
    create_entry,
    delete_entry,
    get_accounts,
    get_categories,
    get_journal,
    update_entry,
)
from app.services.statement_parser import classify_entries, parse_statement

router = APIRouter()


# --- Schemas ---

class JournalEntryCreate(BaseModel):
    entry_type: str  # expense / income / transfer
    amount: float
    nds_amount: float = 0
    is_recurring: bool = False
    recurrence_rule: Optional[str] = None
    recurrence_day: Optional[int] = None
    scheduled_date: Optional[date] = None
    backfill_from: Optional[date] = None
    account_name: str
    category: Optional[str] = None
    counterparty: Optional[str] = None
    description: Optional[str] = None
    is_distributed: bool = False
    is_official: bool = False
    channel_id: Optional[int] = None


class JournalEntryUpdate(BaseModel):
    entry_type: Optional[str] = None
    amount: Optional[float] = None
    nds_amount: Optional[float] = None
    is_recurring: Optional[bool] = None
    recurrence_rule: Optional[str] = None
    recurrence_day: Optional[int] = None
    scheduled_date: Optional[date] = None
    backfill_from: Optional[date] = None
    account_name: Optional[str] = None
    category: Optional[str] = None
    counterparty: Optional[str] = None
    description: Optional[str] = None
    is_distributed: Optional[bool] = None
    is_official: Optional[bool] = None
    channel_id: Optional[int] = None


class StatementConfirmEntry(BaseModel):
    date: date
    amount: float
    counterparty: str = ""
    description: str = ""
    category: str = "other"
    entry_type: str = "expense"
    account_name: str = ""


class StatementConfirmPayload(BaseModel):
    entries: List[StatementConfirmEntry]
    account_name: str


# --- Endpoints ---

@router.get("")
def list_journal(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    entry_type: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    account: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Список операций журнала с фильтрами."""
    return get_journal(db, date_from=date_from, date_to=date_to, entry_type=entry_type, category=category, account=account)


@router.post("")
def create_journal_entry(
    payload: JournalEntryCreate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Создать операцию в журнале."""
    data = payload.dict()
    result = create_entry(db, data, user_id=user.id)
    return result


@router.get("/accounts")
def list_accounts(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Список счетов из DDSBalance."""
    return get_accounts(db)


@router.get("/categories")
def list_categories(
    _: User = Depends(get_current_user),
):
    """Список категорий ДДС."""
    return get_categories()


@router.put("/{entry_id}")
def update_journal_entry(
    entry_id: int,
    payload: JournalEntryUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Обновить операцию журнала."""
    data = payload.dict(exclude_unset=True)
    result = update_entry(db, entry_id, data)
    if result is None:
        raise HTTPException(status_code=404, detail="Entry not found")
    return result


@router.delete("/{entry_id}")
def delete_journal_entry(
    entry_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Удалить операцию журнала."""
    ok = delete_entry(db, entry_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"ok": True, "deleted_id": entry_id}


@router.post("/upload-statement")
async def upload_statement(
    file: UploadFile = File(...),
    _: User = Depends(get_current_user),
):
    """Загрузить банковскую выписку (xlsx/csv), получить предпросмотр с авто-классификацией."""
    if not file.filename:
        raise HTTPException(400, "No file provided")

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ("xlsx", "csv"):
        raise HTTPException(400, f"Unsupported file format: {ext}. Expected xlsx or csv.")

    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Empty file")

    try:
        rows = parse_statement(contents, file.filename)
    except Exception as e:
        raise HTTPException(400, f"Failed to parse file: {str(e)}")

    if not rows:
        raise HTTPException(400, "No data found in file")

    classified = classify_entries(rows)

    return {
        "filename": file.filename,
        "total_rows": len(classified),
        "entries": classified,
    }


@router.post("/upload-confirm")
def confirm_upload(
    payload: StatementConfirmPayload,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Подтвердить импорт операций из банковской выписки."""
    created = []
    for entry_data in payload.entries:
        data = {
            "entry_type": entry_data.entry_type,
            "amount": entry_data.amount,
            "nds_amount": 0,
            "scheduled_date": entry_data.date,
            "account_name": entry_data.account_name or payload.account_name,
            "category": entry_data.category,
            "counterparty": entry_data.counterparty,
            "description": entry_data.description,
            "is_recurring": False,
        }
        result = create_entry(db, data, user_id=user.id)
        created.append(result)

    return {
        "ok": True,
        "created_count": len(created),
        "entries": created,
    }
