ÖNEMLİ:

Database kurulumu ve mock data için:
python -m app.db.init_db
python -m app.db.seeder

Testler için (local):
pip install pytest-dotenv
pytest -v -m integration --maxfail=1