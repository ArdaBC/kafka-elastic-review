from typing import Set

def _get_migrations_collection(db):
    return db["migrations"]


def has_migration_run(db, name: str) -> bool:
    return _get_migrations_collection(db).find_one({"name": name}) is not None


def record_migration(db, name: str):
    _get_migrations_collection(db).insert_one({"name": name})


def list_applied_migrations(db) -> Set[str]:
    docs = _get_migrations_collection(db).find({}, {"name": 1})
    return {d["name"] for d in docs}
