"""
Backup persistence and optional e-mail notification for processed datasets.
"""

from __future__ import annotations

import os
import shutil
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Optional, Tuple

import streamlit as st


def _get_secret_or_env(key: str, default: str = "") -> str:
    env_val = os.getenv(key, "").strip()
    if env_val:
        return env_val
    try:
        secret_val = str(st.secrets[key]).strip()
        if secret_val:
            return secret_val
    except Exception:
        pass
    return default


def persist_processed_backup(processed_file_path: Path) -> Tuple[bool, str, Optional[Path]]:
    """
    Save a versioned copy of the processed parquet in local backups folder.
    """
    try:
        source = Path(processed_file_path)
        if not source.exists():
            return False, "Arquivo processado não encontrado para backup.", None

        backup_dir = Path("dados_cliente/backups")
        backup_dir.mkdir(parents=True, exist_ok=True)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{source.stem}_{ts}{source.suffix or '.parquet'}"
        backup_path = backup_dir / backup_name
        shutil.copy2(source, backup_path)
        return True, f"Backup salvo em {backup_path}", backup_path
    except Exception as exc:
        return False, f"Falha ao salvar backup: {exc}", None


def send_backup_email(backup_file_path: Path, recipient: str = "") -> Tuple[bool, str]:
    """
    Send backup file by e-mail using SMTP credentials from env/secrets.
    """
    smtp_host = _get_secret_or_env("INSIGHTX_SMTP_HOST")
    smtp_port = int(_get_secret_or_env("INSIGHTX_SMTP_PORT", "587"))
    smtp_user = _get_secret_or_env("INSIGHTX_SMTP_USER")
    smtp_pass = _get_secret_or_env("INSIGHTX_SMTP_PASS")
    from_email = _get_secret_or_env("INSIGHTX_SMTP_FROM", smtp_user)
    target_email = recipient.strip() or _get_secret_or_env("INSIGHTX_BACKUP_NOTIFY_EMAIL")

    if not target_email:
        return False, "E-mail de destino não configurado (INSIGHTX_BACKUP_NOTIFY_EMAIL)."
    if not (smtp_host and smtp_user and smtp_pass and from_email):
        return False, "SMTP não configurado (INSIGHTX_SMTP_HOST/USER/PASS/FROM)."
    if not backup_file_path.exists():
        return False, "Arquivo de backup não encontrado para envio."

    try:
        msg = EmailMessage()
        msg["Subject"] = f"Backup Insight Expert - {datetime.now():%d/%m/%Y %H:%M}"
        msg["From"] = from_email
        msg["To"] = target_email
        msg.set_content(
            "Segue em anexo a cópia do arquivo processado pelo motor do Insight Expert."
        )

        file_bytes = backup_file_path.read_bytes()
        msg.add_attachment(
            file_bytes,
            maintype="application",
            subtype="octet-stream",
            filename=backup_file_path.name,
        )

        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        return True, f"Backup enviado para {target_email}."
    except Exception as exc:
        return False, f"Falha ao enviar e-mail: {exc}"


def persist_backup_and_notify(
    processed_file_path: Path, recipient: str = ""
) -> Tuple[bool, str, Optional[Path]]:
    """
    Persist backup and try to notify by e-mail.
    """
    ok, msg, backup_path = persist_processed_backup(processed_file_path)
    if not ok or backup_path is None:
        return False, msg, backup_path

    sent, send_msg = send_backup_email(backup_path, recipient=recipient)
    if sent:
        return True, f"{msg} | {send_msg}", backup_path

    # Backup persisted even when e-mail fails.
    return False, f"{msg} | {send_msg}", backup_path
