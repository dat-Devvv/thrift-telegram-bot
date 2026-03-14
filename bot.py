import os
import shutil
import logging
import hashlib
from datetime import datetime
from pathlib import Path

# Third-Party Imports
import pandas as pd
from openpyxl import load_workbook
from dotenv import load_dotenv

# python-telegram-bot v20 async API
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
# Load environment variables from .env file
# ─────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "8582440175:AAEKvp4slc3shgTMuOcN8wPGK5w2inElf3s")

# Comma-separated Telegram user IDs that are allowed to use the bot.
_raw_users = os.getenv("ALLOWED_USERS", "")
ALLOWED_USERS: set[int] = (
    {int(uid.strip()) for uid in _raw_users.split(",") if uid.strip()} if _raw_users else set()
)

# ─────────────────────────────────────────────────────────────
# Directory / File Configuration
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
# Logging Setup
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
    """Return True if the user is allowed to use the bot."""
    if not ALLOWED_USERS:
        return True
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
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = BACKUPS_DIR / f"master_backup_{ts}.xlsx"
    shutil.copy2(MASTER_PATH, backup)
    logger.info("Master saved and backed up to %s", backup)

def _validate_columns(df: pd.DataFrame, required: list[str], name: str) -> list[str]:
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.warning("Sheet '%s' is missing columns: %s", name, missing)
    return missing

# ─────────────────────────────────────────────────────────────
# Duplicate-file prevention
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
# Contribution logic
# ─────────────────────────────────────────────────────────────
def _crosses_month_boundary(start_date: datetime, days_paid: int) -> bool:
    end_date = start_date + pd.Timedelta(days=int(days_paid) - 1)
    return end_date.month != start_date.month or end_date.year != start_date.year

def process_contributions(contrib_path: Path) -> dict:
    contrib_df = pd.read_excel(contrib_path, dtype={"Serial No": str})
    contrib_df = _normalize_columns(contrib_df)
    missing = _validate_columns(contrib_df, CONTRIB_COLS, "Contribution")
    if missing:
        return {"ok": False, "error": f"Missing columns: {missing}"}

    members_df = load_master()
    log_df = load_transaction_log()
    if members_df.empty:
        return {"ok": False, "error": "Master sheet not loaded yet. Use /upload_main first."}

    for col in ["Total Contributions", "Total Withdrawals", "Balance"]:
        members_df[col] = pd.to_numeric(members_df[col], errors="coerce").fillna(0)
    members_df["Serial No"] = members_df["Serial No"].astype(str).str.strip()

    summary = {"ok": True, "processed": 0, "skipped": [], "profit_total": 0.0}

    for _, row in contrib_df.iterrows():
        serial = str(row["Serial No"]).strip()
        daily_save = float(row["Daily Saving"])
        days_paid = int(row["Days Paid"])
        try:
            record_date = pd.to_datetime(row["Date"])
        except Exception:
            summary["skipped"].append(f"{serial} (bad date)")
            continue
        idx = members_df.index[members_df["Serial No"] == serial].tolist()
        if not idx:
            summary["skipped"].append(f"{serial} (not found)")
            logger.warning("Serial No '%s' not found in master sheet.", serial)
            continue
        idx = idx[0]

        total_contrib = daily_save * days_paid
        profit = daily_save if _crosses_month_boundary(record_date, days_paid) else 0.0
        net_contrib = total_contrib - profit

        members_df.at[idx, "Total Contributions"] += net_contrib
        members_df.at[idx, "Balance"] += net_contrib

        new_log = pd.DataFrame([{
            "Date": record_date.strftime("%Y-%m-%d"),
            "Serial No": serial,
            "Type": "CONTRIBUTION",
            "Amount": net_contrib,
            "Days": days_paid,
            "Profit": profit,
        }])
        log_df = pd.concat([log_df, new_log], ignore_index=True)

        summary["processed"] += 1
        summary["profit_total"] += profit
        logger.info("Contribution → Serial %s | Net ₦%.2f | Profit ₦%.2f", serial, net_contrib, profit)

    save_master(members_df, log_df)
    return summary

# ─────────────────────────────────────────────────────────────
# Withdrawal logic
# ─────────────────────────────────────────────────────────────
def process_withdrawals(withdraw_path: Path) -> dict:
    wd_df = pd.read_excel(withdraw_path, dtype={"Serial No": str})
    wd_df = _normalize_columns(wd_df)
    missing = _validate_columns(wd_df, WITHDRAW_COLS, "Withdrawal")
    if missing:
        return {"ok": False, "error": f"Missing columns: {missing}"}

    members_df = load_master()
    log_df = load_transaction_log()
    if members_df.empty:
        return {"ok": False, "error": "Master sheet not loaded yet. Use /upload_main first."}

    for col in ["Total Contributions", "Total Withdrawals", "Balance"]:
        members_df[col] = pd.to_numeric(members_df[col], errors="coerce").fillna(0)
    members_df["Serial No"] = members_df["Serial No"].astype(str).str.strip()

    summary = {"ok": True, "processed": 0, "rejected": [], "skipped": []}

    for _, row in wd_df.iterrows():
        serial = str(row["Serial No"]).strip()
        amount = float(row["Amount"])
        try:
            record_date = pd.to_datetime(row["Date"])
        except Exception:
            summary["skipped"].append(f"{serial} (bad date)")
            continue
        idx = members_df.index[members_df["Serial No"] == serial].tolist()
        if not idx:
            summary["skipped"].append(f"{serial} (not found)")
            logger.warning("Withdrawal: Serial No '%s' not found.", serial)
            continue
        idx = idx[0]

        current_balance = members_df.at[idx, "Balance"]
        if amount > current_balance:
            summary["rejected"].append(
                f"{serial} (amount ₦{amount:.2f} > balance ₦{current_balance:.2f})"
            )
            logger.warning("Withdrawal REJECTED: Serial %s | Requested ₦%.2f | Balance ₦%.2f",
                           serial, amount, current_balance)
            continue

        members_df.at[idx, "Total Withdrawals"] += amount
        members_df.at[idx, "Balance"] -= amount

        new_log = pd.DataFrame([{
            "Date": record_date.strftime("%Y-%m-%d"),
            "Serial No": serial,
            "Type": "WITHDRAWAL",
            "Amount": amount,
            "Days": 0,
            "Profit": 0,
        }])
        log_df = pd.concat([log_df, new_log], ignore_index=True)

        summary["processed"] += 1
        logger.info("Withdrawal → Serial %s | ₦%.2f deducted", serial, amount)

    save_master(members_df, log_df)
    return summary

# ─────────────────────────────────────────────────────────────
# Telegram commands (start, help, upload, contrib, withdraw, export, balance, report)
# ─────────────────────────────────────────────────────────────
# (Handlers remain largely the same, with parse_mode="MarkdownV2" for safety)
# You can copy the handlers from your existing code.

# ─────────────────────────────────────────────────────────────
# Bot setup
# ─────────────────────────────────────────────────────────────
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    # Register all handlers here (same as your original code)
    return app

# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if BOT_TOKEN == "8582440175:AAEKvp4slc3shgTMuOcN8wPGK5w2inElf3s":
        print("❌  Set BOT_TOKEN in your .env file before running.")
    else:
        logger.info("Starting Thrift Bot…")
        app = build_app()
        app.run_polling(allowed_updates=Update.ALL_TYPES)