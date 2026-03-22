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


@router.post("/migrate-data")
def migrate_data(
    payload: dict,
    db: Session = Depends(get_db),
):
    """Временный endpoint для миграции данных. Принимает SQL."""
    sql = payload.get("sql", "")
    if not sql:
        raise HTTPException(400, "No SQL provided")
    try:
        db.execute(text(sql))
        db.commit()
        return {"ok": True, "length": len(sql)}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:500]}
