ÖNEMLİ:

Database kurulumu ve mock data için:
python -m app.db.init_db
python -m app.db.seeder

Testler için:
pip install pytest-dotenv
pytest -m integration