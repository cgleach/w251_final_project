from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import os

load_dotenv( verbose = True)

SECRET_KEY = os.getenv("ACCESS_KEY")
print(SECRET_KEY)

