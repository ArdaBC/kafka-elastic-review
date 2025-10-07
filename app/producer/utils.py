import random
from datetime import datetime, timedelta

users = [
    "I",
    "We",
    "My partner",
    "My spouse",
    "My girlfriend",
    "My boyfriend",
    "A friend",
    "A close friend",
    "My best friend",
    "My roommate",
    "My colleague",
    "My boss",
    "My coworker",
    "My teammate",
    "My mom",
    "My dad",
    "My brother",
    "My sister",
    "My son",
    "My daughter",
    "My kids",
    "My family",
    "My neighbor",
    "My cousin",
    "My uncle",
    "My aunt",
    "My grandparents",
    "My grandpa",
    "My grandma",
]

adjectives_pos = [
    "excellent",
    "fantastic",
    "outstanding",
    "solid",
    "reliable",
    "amazing",
    "impressive",
    "durable",
    "comfortable",
    "sleek",
    "modern",
    "remarkable",
    "incredible",
    "smooth",
    "top-notch",
    "affordable",
    "user-friendly",
    "well-designed",
    "consistent",
    "versatile",
]

adjectives_neg = [
    "terrible",
    "disappointing",
    "cheap",
    "unreliable",
    "buggy",
    "frustrating",
    "awkward",
    "clunky",
    "uncomfortable",
    "outdated",
    "horrible",
    "mediocre",
    "inconsistent",
    "slow",
    "laggy",
    "poorly designed",
    "overpriced",
    "fragile",
    "noisy",
    "unresponsive",
]

features = [
    "battery life",
    "screen",
    "sound quality",
    "customer service",
    "build quality",
    "design",
    "performance",
    "comfort",
    "durability",
    "price",
    "ease of use",
    "packaging",
    "delivery speed",
    "size",
    "weight",
    "controls",
    "instructions",
    "software",
    "connection stability",
    "overall value",
]

verbs_pos = [
    "exceeded",
    "met",
    "surprised",
    "outperformed",
    "delivered on",
    "matched",
    "impressed",
    "amazed",
    "improved",
    "satisfied",
    "delighted",
    "shocked",
    "thrilled",
    "wow-ed",
    "blew us away",
    "lived up to expectations",
    "worked flawlessly",
    "made a difference",
    "went above and beyond",
    "proved reliable",
    "felt premium",
    "enhanced",
]

verbs_neg = [
    "fell short of",
    "disappointed",
    "let down",
    "annoyed",
    "failed to deliver",
    "ruined",
    "underwhelmed",
    "frustrated",
    "struggled",
    "underperformed",
    "broke",
    "lagged",
    "caused trouble",
    "felt cheap",
    "was problematic",
    "didn't meet expectations",
    "was unstable",
    "underwhelmed me",
    "made things worse",
    "was confusing",
]

templates = [
    "{user} bought this for {purpose} and it {verb} our expectations. The {feature} is {adj}.",
    "{user} has used this product for {months} months. Overall {adj}. Would {recommend}.",
    "If you care about {feature}, this is a {adj} choice. {reason}",
    "{user} just started using it and was immediately {verb}. The {feature} feels {adj}.",
    "At first glance, the {feature} looks {adj}, and after some use it still {verb} expectations.",
    "I wasn't sure at first, but the {feature} turned out to be {adj}.",
    "Compared to other products, the {feature} here is surprisingly {adj}.",
    "This was supposed to replace my old one, and while the {feature} is {adj}, it {verb} in other areas.",
    "I've tried similar items, but none {verb} like this one. The {feature} is {adj}.",
    "Honestly, if {purpose} is what you need, this product is {adj}.",
    "The {feature} is {adj}, so I'd definitely {recommend}.",
    "For anyone looking for {purpose}, this feels like a {adj} option.",
    "After {months} months, I can say the {feature} has stayed {adj}.",
    "Over time, the {feature} became more {adj}, which really {verb} me.",
    "Even after daily use for {months} months, the {feature} is still {adj}.",
    "{user} had an issue at first but customer service was {adj}, so overall experience {verb}.",
    "The delivery was quick and the packaging was {adj}. The {feature} itself also {verb}.",
    "I contacted support, and they were {adj}. That made the {feature} feel even better.",
    "The {feature} is simply {adj}.",
    "{user} was {verb} by how {adj} the {feature} is.",
    "Overall: {adj}. Would {recommend}.",
    "The {feature} is {adj}, though other parts {verb} expectations.",
    "Some aspects are {adj}, but others {verb}. Still, {recommend}.",
    "Mixed feelings: the {feature} is {adj}, but it also {verb} me.",
]


def random_timestamp(start_days_ago=120):
    now = datetime.now()
    start = now - timedelta(days=start_days_ago)
    delta = now - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


# odds are % of pos review
def generate_review(odds=50):

    sentiment = random.choices(
        population=["positive", "negative"], weights=[odds, 100 - odds], k=1
    )[0]

    rating = (
        random.randint(7, 10) / 2
        if sentiment == "positive"
        else random.randint(2, 7) / 2
    )

    adj = random.choice(adjectives_pos if sentiment == "positive" else adjectives_neg)
    verb = random.choice(verbs_pos if sentiment == "positive" else verbs_neg)
    feature = random.choice(features)
    user = random.choice(users)
    months = random.choice(["two", "three", "six", "twelve"])
    purpose = random.choice(["daily use", "travel", "work", "gifts"])
    recommend = "recommend it" if sentiment == "positive" else "not recommend it"
    reason = (
        "It performs well and is worth the money."
        if sentiment == "positive"
        else "It keeps crashing and support was unhelpful."
    )

    template = random.choice(templates)
    review_text = template.format(
        user=user,
        purpose=purpose,
        verb=verb,
        feature=feature,
        adj=adj,
        months=months,
        recommend=recommend,
        reason=reason,
    )

    return {
        "user_id": random.randint(1, 200),
        "product_id": random.randint(1, 100),
        "rating": rating,
        "review_text": review_text,
        "timestamp": random_timestamp().isoformat(),
    }
