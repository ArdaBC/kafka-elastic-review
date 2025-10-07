import os
import runpy
from app.db import utils
from app.services.db import get_db
from dotenv import load_dotenv

load_dotenv()

SKIP_MIGRATIONS = os.getenv("SKIP_MIGRATIONS", "false").lower() == "true"

if SKIP_MIGRATIONS:
    print("SKIP_MIGRATIONS is set. Exiting without running migrations.")
    exit(0)

MIGRATIONS_DIR = os.getenv(
    "MIGRATIONS_DIR", os.path.join(os.path.dirname(__file__), "migrations")
)


def _sorted_migration_paths():
    files = [
        f
        for f in os.listdir(MIGRATIONS_DIR)
        if f.endswith(".py") and not f.startswith("__")
    ]
    files.sort()
    return [os.path.join(MIGRATIONS_DIR, f) for f in files]


def run_migrations():
    db = get_db()
    applied = utils.list_applied_migrations(db)

    for path in _sorted_migration_paths():
        name = os.path.basename(path)
        if name in applied:
            print(f"Skipping already applied migration: {name}")
            continue

        print(f"Applying migration: {name}")
        module_globals = runpy.run_path(path)
        if "run" not in module_globals:
            raise RuntimeError(
                f"Migration file {name} does not define a run(db) function."
            )
        module_globals["run"](db)
        utils.record_migration(db, name)
        print(f"Recorded migration: {name}")

    print("All migrations processed.")


if __name__ == "__main__":
    run_migrations()
