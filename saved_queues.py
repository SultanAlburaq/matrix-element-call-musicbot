import json
import os
from pathlib import Path
import tempfile
from typing import Optional


class SavedQueueStore:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_all(self) -> dict:
        if not self.file_path.exists():
            return {"rooms": {}}
        try:
            data = json.loads(self.file_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"rooms": {}}
        if not isinstance(data, dict):
            return {"rooms": {}}
        rooms = data.get("rooms")
        if not isinstance(rooms, dict):
            data["rooms"] = {}
        return data

    def _save_all(self, data: dict):
        payload = json.dumps(data, indent=2, ensure_ascii=True)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=f".{self.file_path.name}.", suffix=".tmp", dir=str(self.file_path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
                tmp_file.write(payload)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            os.replace(tmp_path, self.file_path)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except OSError:
                pass

    def list_names(self, room_id: str) -> list[str]:
        data = self._load_all()
        room_data = data.get("rooms", {}).get(room_id, {})
        if not isinstance(room_data, dict):
            return []
        return sorted(room_data.keys())

    def save_queue(self, room_id: str, name: str, tracks: list[dict]):
        data = self._load_all()
        rooms = data.setdefault("rooms", {})
        room_data = rooms.setdefault(room_id, {})
        room_data[name] = {
            "tracks": tracks,
        }
        self._save_all(data)

    def has_queue(self, room_id: str, name: str) -> bool:
        data = self._load_all()
        room_data = data.get("rooms", {}).get(room_id, {})
        return isinstance(room_data, dict) and name in room_data

    def load_queue(self, room_id: str, name: str) -> Optional[list[dict]]:
        data = self._load_all()
        room_data = data.get("rooms", {}).get(room_id, {})
        if not isinstance(room_data, dict):
            return None
        entry = room_data.get(name)
        if not isinstance(entry, dict):
            return None
        tracks = entry.get("tracks")
        if not isinstance(tracks, list):
            return None
        return tracks

    def delete_queue(self, room_id: str, name: str) -> bool:
        data = self._load_all()
        room_data = data.get("rooms", {}).get(room_id, {})
        if not isinstance(room_data, dict) or name not in room_data:
            return False
        del room_data[name]
        self._save_all(data)
        return True

    def rename_queue(self, room_id: str, old_name: str, new_name: str) -> tuple[bool, str]:
        data = self._load_all()
        room_data = data.get("rooms", {}).get(room_id, {})
        if not isinstance(room_data, dict):
            return False, "missing_old"
        if old_name not in room_data:
            return False, "missing_old"
        if new_name in room_data:
            return False, "new_exists"

        room_data[new_name] = room_data.pop(old_name)
        self._save_all(data)
        return True, "ok"
