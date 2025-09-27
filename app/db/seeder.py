import os
import hashlib
import binascii
import random
from app.services.db import get_db

first_names = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Edward", "Deborah"
]

surnames = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
    "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell",
    "Mitchell", "Carter"
]

def _hash_password(plain_password: str, salt: bytes = None, iterations: int = 100_000):
    if salt is None:
        salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", plain_password.encode("utf-8"), salt, iterations)
    return {
        "salt": binascii.hexlify(salt).decode("ascii"),
        "hash": binascii.hexlify(dk).decode("ascii"),
        "iterations": iterations,
        "algo": "pbkdf2_hmac_sha256"
    }


def seed_users(db, count=200):
    for user_id in range(1, count + 1):
        first = random.choice(first_names)
        last = random.choice(surnames)
        user_doc = {
            "user_id": user_id,
            "name": f"{first} {last}",
            "email": f"user{user_id}@example.com",
            "phone": f"555 000 {user_id:04d}",
            "password": _hash_password(f"password{user_id}"),
            "total_reviews": 0,
            "average_rating": 0.0,
        }
        db["users"].update_one({"user_id": user_id}, {"$set": user_doc}, upsert=True)
    print(f"{count} users seeded/upserted.")


def seed_products(db, count=100):
    for product_id in range(1, count + 1):
        product_doc = {
            "product_id": product_id,
            "name": f"Product Name {product_id}",
            "total_ratings": 0,
            "average_rating": 0.0,
            "total_reviews": 0,
        }
        db["products"].update_one({"product_id": product_id}, {"$set": product_doc}, upsert=True)
    print(f"{count} products seeded/upserted.")


def run():
    db = get_db()
    seed_users(db)
    seed_products(db)
    print("Seeder finished.")


if __name__ == "__main__":
    run()