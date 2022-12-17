from dotenv import load_dotenv
import os

def configure():
    load_dotenv()

# this function will fetch mongo db username from .env
def get_mongodb_username():
    return os.environ["MONGO_USER_NAME"]

# this function will fetch mongo db password from .env
def get_mongodb_password():
    return os.environ["MONGO_PASSWORD"]
