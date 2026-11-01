"""JWT-based tenant authentication — uses bcrypt directly (no passlib)."""
import os
from datetime import datetime, timedelta
from typing import Optional

import bcrypt
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from pydantic import BaseModel

from app.db import query

SECRET_KEY           = os.environ.get("JWT_SECRET", "change-me-in-production-32chars")
ALGORITHM            = "HS256"
TOKEN_EXPIRE_MINUTES = int(os.environ.get("TOKEN_EXPIRE_MINUTES", 60))

bearer = HTTPBearer(auto_error=False)

class LoginRequest(BaseModel):
    tenant_id: int
    password:  str

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    tenant_id:    int
    expires_in:   int = TOKEN_EXPIRE_MINUTES * 60

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()

def _verify(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False

def _create_token(tenant_id: int) -> str:
    exp = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    return jwt.encode({"sub": str(tenant_id), "exp": exp}, SECRET_KEY, algorithm=ALGORITHM)

def do_login(req: LoginRequest) -> TokenResponse:
    row = query("SELECT password_hash FROM tenants WHERE tenant_id=%s",
                (req.tenant_id,), one=True)
    if not row or not row.get("password_hash") or not _verify(req.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid tenant_id or password")
    return TokenResponse(access_token=_create_token(req.tenant_id), tenant_id=req.tenant_id)

def get_current_tenant(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(bearer),
) -> int:
    if creds is None:
        raise HTTPException(status_code=401, detail="Missing Bearer token. POST /auth/login first.",
                            headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(creds.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        sub = payload.get("sub")
        if sub is None: raise ValueError
        return int(sub)
    except (JWTError, ValueError):
        raise HTTPException(status_code=401, detail="Invalid or expired token.",
                            headers={"WWW-Authenticate": "Bearer"})

def require_tenant(tenant_id_from_path: int, current: int) -> int:
    if current != tenant_id_from_path:
        raise HTTPException(status_code=403,
            detail=f"Token is for tenant {current}, not tenant {tenant_id_from_path}.")
    return current