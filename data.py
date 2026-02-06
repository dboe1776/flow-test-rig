import httpx
import asyncio
import models
import datetime as dt
import json
import csv
from pathlib import Path
from loguru import logger

class DataManager:

    def __init__(self,configs:list[models.AnyDataSink]):
        self.configs = configs 
        self.handlers: list[BaseDataHandler] = []       
        self.register_handlers()

    def register_handlers(self):
        for sink in self.configs:
            if not sink.enabled:
                continue
            if isinstance(sink, models.JsonFolderSink):
                handler = JsonHandler(sink)
            elif isinstance(sink, models.CsvSink):
                handler = CsvHandler(sink)
            elif isinstance(sink, models.InfluxSink):
                # handler = InfluxHandler(sink)  # you'll implement this later
                pass
            else:
                continue

            handler.initialize()
            self.handlers.append(handler)

    async def handle_data(self,record:models.TestRigDF):
        for handler in self.handlers:
            try:
                await handler.handle(record)
            except Exception as e:
                logger.error(f'Error in data handling {e}')

class BaseDataHandler:
    """Base class for all data handlers.

    Subclasses should implement the `handle` method and set `type`.
    """

    type: str = ""

    def __init__(self, config: models.AnyDataSink):
        self.config = config
        self._initialized = False

    def initialize(self) -> None:
        """Called once before first handle() — can be overridden."""
        pass

    async def handle(self, record: models.TestRigDF) -> None:
        """Process one measurement record.

        Subclasses must implement this.
        """
        raise NotImplementedError("Subclasses must implement handle()")

    async def close(self) -> None:
        """Optional cleanup hook (files, connections, etc.)."""
        pass

class JsonHandler(BaseDataHandler):
    """Writes records as newline-delimited JSON (JSONL / NDJSON).

    Creates one new file per handler instance (per app start).
    """

    type = "json_folder"

    def __init__(self, config: models.JsonFolderSink):
        super().__init__(config)

        if not isinstance(config.folder,Path):
            folder = Path(config.folder)
        else:
            folder = config.folder
        folder.mkdir(exist_ok=True, parents=True)

        # File name with timestamp + optional name from config
        timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        stem = f"{timestamp}_{config.name or 'records'}"
        self.filepath = folder / f"{stem}.jsonl"

        logger.info(f"JSON handler writing to: {self.filepath}")

    def initialize(self) -> None:
        # Optional: touch file or write header comment
        self.filepath.touch(exist_ok=True)

    async def handle(self, record: models.TestRigDF) -> None:
        try:
            # Convert record to dict (adjust based on your TestRigDF structure)
            data = record.flatten()

            # Append as JSON line
            line = json.dumps(data, default=str) + "\n"  # str fallback for datetime/Path/etc.

            # Simple async file append (or use aiofiles if you prefer)
            async with asyncio.Lock():  # protect concurrent writes if needed
                with self.filepath.open("a", encoding="utf-8") as f:
                    f.write(line)

        except Exception as e:
            logger.exception(f"Failed to write JSON record to {self.filepath}: {e}")

    async def close(self) -> None:
        logger.info(f"JSON handler closed file: {self.filepath}")

class CsvHandler(BaseDataHandler):
    """Writes records as CSV.

    Creates one new file per handler instance with header on first write.
    """

    type = "csv_file"

    def __init__(self, config: models.CsvSink):  # ← you'll need to define CsvSink in models
        super().__init__(config)

        folder = Path(config.folder)
        folder.mkdir(exist_ok=True, parents=True)

        timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        stem = f"{timestamp}_{config.name or 'records'}"
        self.filepath = folder / f"{stem}.csv"

        self._header_written = False
        self._fieldnames: list[str] | None = None  # set on first record

        logger.info(f"CSV handler writing to: {self.filepath}")

    async def handle(self, record: models.TestRigDF) -> None:
        try:
            # Convert to dict
            data = record.flatten()

            # Determine fieldnames from first record
            if not self._header_written:
                self._fieldnames = list(data.keys())
                self._header_written = True

                with self.filepath.open("w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=self._fieldnames)
                    writer.writeheader()

            # Append row
            with self.filepath.open("a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self._fieldnames)
                writer.writerow(data)

        except Exception as e:
            logger.exception(f"Failed to write CSV record to {self.filepath}: {e}")

    async def close(self) -> None:
        logger.info(f"CSV handler closed file: {self.filepath}")        