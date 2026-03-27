from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.core.security import create_access_token, verify_password
from app.models.user import User
from app.schemas.auth import LoginRequest, TokenResponse, UserOut

router = APIRouter()


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == payload.email).first()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="User is inactive")
    token = create_access_token({"sub": str(user.id)})
    return TokenResponse(access_token=token)


@router.get("/me", response_model=UserOut)
def me(current_user: User = Depends(get_current_user)):
    return current_user


@router.post("/resync-expenses")
def resync_expenses(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Ресинк финотчёта WB (полный перезалив credit_deduction)."""
    from app.models.integration import Integration, IntegrationType
    from app.services.wb_api import WBClient
    from app.services.wb_sync import sync_wb_expenses

    integration = db.query(Integration).filter(Integration.type == IntegrationType.WB).first()
    if not integration:
        raise HTTPException(404, "WB integration not found")
    client = WBClient(integration.api_key)
    sync_wb_expenses(db, client, days_back=90)
    return {"ok": True, "message": "WB expenses resynced (90 days)"}


