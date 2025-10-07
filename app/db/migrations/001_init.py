"""
Migration 001 - initial collections and indexes.

Creates the three collections (users, products, reviews)

It must define a function "run(db)" that the migration runner will call.
"""


def run(db):

    existing = set(db.list_collection_names())

    if "users" not in existing:
        db.create_collection("users")
    if "products" not in existing:
        db.create_collection("products")
    if "reviews" not in existing:
        db.create_collection("reviews")

    # Users indexes
    db["users"].create_index("user_id", unique=True, background=True)
    db["users"].create_index("email", unique=True, background=True)

    # Products indexes
    db["products"].create_index("product_id", unique=True, background=True)

    # Reviews indexes
    db["reviews"].create_index("user_id", background=True)
    db["reviews"].create_index("product_id", background=True)
    db["reviews"].create_index("timestamp", background=True)

    print("Migration 001: created collections and indexes.")
