import os
import shutil
import logging
from datetime import datetime
from pathlib import Path
import hashlib

# Third-party libraries
import pandas as pd
from openpyxl import load_workbook
from telegram import Update, Document
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)

# ─────────────────────────────────────────────────────────────
# Telegram Bot Token
# ─────────────────────────────────────────────────────────────

BOT_TOKEN = "8582440175:AAEKvp4slc3shgTMuOcN8wPGK5w2inElf3s"

# Allowed users (optional, leave empty to allow everyone)
ALLOWED_USERS = set()  # e.g., {123456789, 987654321}

# ─────────────────────────────────────────────────────────────
# Directories
# ─────────────────────────────────────────────────────────────

BASE_DIR = Path("thrift_data")
UPLOADS_DIR = BASE_DIR / "uploads"
BACKUPS_DIR = BASE_DIR / "backups"
PROCESSED_DIR = BASE_DIR / "processed"
MASTER_PATH = BASE_DIR / "master.xlsx"
PROCESSED_LOG = BASE_DIR / "processed_files.txt"

for _d in (BASE_DIR, UPLOADS_DIR, BACKUPS_DIR, PROCESSED_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Conversation states
# ─────────────────────────────────────────────────────────────

AWAIT_MAIN = 1
AWAIT_CONTRIB = 2
AWAIT_WITHDRAW = 3

# ─────────────────────────────────────────────────────────────
# Authorization helpers
# ─────────────────────────────────────────────────────────────

def is_authorized(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True  # no restriction
    return user_id in ALLOWED_USERS

async def guard(update: Update) -> bool:
    if not is_authorized(update.effective_user.id):
        await update.message.reply_text("⛔ You are not authorized to use this bot.")
        return False
    return True

# ─────────────────────────────────────────────────────────────
# Spreadsheet helpers
# ─────────────────────────────────────────────────────────────

MASTER_COLS = ["Serial No", "Name", "Daily Amount", "Total Contributions", "Total Withdrawals", "Balance"]
CONTRIB_COLS = ["Date", "Serial No", "Daily Saving", "Days Paid"]
WITHDRAW_COLS = ["Date", "Serial No", "Amount"]
LOG_COLS = ["Date", "Serial No", "Type", "Amount", "Days", "Profit"]

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    return df

def load_master() -> pd.DataFrame:
    if not MASTER_PATH.exists():
        return pd.DataFrame(columns=MASTER_COLS)
    df = pd.read_excel(MASTER_PATH, sheet_name="Members", dtype={"Serial No": str})
    return _normalize_columns(df)

def load_transaction_log() -> pd.DataFrame:
    if not MASTER_PATH.exists():
        return pd.DataFrame(columns=LOG_COLS)
    try:
        df = pd.read_excel(MASTER_PATH, sheet_name="TransactionLog", dtype={"Serial No": str})
        return _normalize_columns(df)
    except Exception:
        return pd.DataFrame(columns=LOG_COLS)

def save_master(members_df: pd.DataFrame, log_df: pd.DataFrame) -> None:
    with pd.ExcelWriter(MASTER_PATH, engine="openpyxl") as writer:
        members_df.to_excel(writer, sheet_name="Members", index=False)
        log_df.to_excel(writer, sheet_name="TransactionLog", index=False)
    # Backup
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = BACKUPS_DIR / f"master_backup_{ts}.xlsx"
    shutil.copy2(MASTER_PATH, backup)
    logger.info("Master saved and backed up to %s", backup)

# ─────────────────────────────────────────────────────────────
# File duplication prevention
# ─────────────────────────────────────────────────────────────

def _file_hash(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _is_processed(path: Path) -> bool:
    digest = _file_hash(path)
    if not PROCESSED_LOG.exists():
        return False
    return digest in PROCESSED_LOG.read_text().splitlines()

def _mark_processed(path: Path) -> None:
    digest = _file_hash(path)
    with open(PROCESSED_LOG, "a") as f:
        f.write(digest + "\n")

# ─────────────────────────────────────────────────────────────
# Contribution / Withdrawal logic
# ─────────────────────────────────────────────────────────────
# Keep your existing process_contributions() and process_withdrawals() code here
# No changes needed

# ─────────────────────────────────────────────────────────────
# Telegram handlers
# ─────────────────────────────────────────────────────────────
# Keep your /start, /help, /upload_main, /contrib, /withdraw, /export, /balance, /report handlers
# No changes needed for Telegram logic

# ─────────────────────────────────────────────────────────────
# Build and run application
# ─────────────────────────────────────────────────────────────

def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    # Add all your handlers here as in your previous code
    return app

if __name__ == "__main__":
    logger.info("Starting Thrift Bot…")
    app = build_app()
    app.run_polling(allowed_updates=Update.ALL_TYPES)