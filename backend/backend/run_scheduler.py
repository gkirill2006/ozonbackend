import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # /workspace/backend
CURRENT_DIR = Path(__file__).resolve().parent     # /workspace/backend/backend

# Собираем sys.path: проектный корень + всё остальное, исключая каталог текущего файла,
# чтобы модуль celery не подхватывал backend/backend/celery.py как top-level "celery".
clean_paths = []
for p in sys.path:
    if p and Path(p).resolve() != CURRENT_DIR:
        clean_paths.append(p)
sys.path = [str(BASE_DIR)] + clean_paths
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')

import django  # noqa: E402

django.setup()

from ozon.models import OzonSupplyBatch  # noqa: E402
from ozon.tasks import process_supply_batch_sync, _cleanup_stale_drafts  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "scheduler.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

PROCESSING_STORES: set[int] = set()
SLEEP_SECONDS = 5


def pick_batches():
    """Вернуть батчи с работой (queued/processing)."""
    return (
        OzonSupplyBatch.objects
        .filter(status__in=["queued", "processing"])
        .select_related("store")
        .order_by("created_at")
    )


def main():
    logger.info("🚀 supply scheduler started")
    while True:
        try:
            batches = list(pick_batches())
            for batch in batches:
                store_id = batch.store_id
                if store_id in PROCESSING_STORES:
                    continue

                PROCESSING_STORES.add(store_id)
                try:
                    logger.info(f"[store={store_id}] ▶️ processing batch {batch.batch_id}")
                    process_supply_batch_sync(str(batch.batch_id))
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"[store={store_id}] ❌ error in batch {batch.batch_id}: {exc}")
                finally:
                    PROCESSING_STORES.discard(store_id)

            # Периодически чистим старые черновики/батчи
            try:
                deleted_drafts, deleted_batches = _cleanup_stale_drafts()
                if deleted_drafts or deleted_batches:
                    logger.info(f"[cleanup] drafts={deleted_drafts} batches={deleted_batches}")
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[cleanup] error: {exc}")

        except Exception as exc:  # noqa: BLE001
            logger.error(f"❌ scheduler loop error: {exc}")

        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
