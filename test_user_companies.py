import os
import sys

sys.path.insert(0, os.path.abspath("."))
from dotenv import load_dotenv
load_dotenv()
os.environ["LOCAL_INFRA"] = "1"
os.environ["db_host"] = "localhost"
os.environ["db_port"] = "5432"
os.environ["db_user"] = "solcpuser"
os.environ["db_password"] = "local_dev_password"
os.environ["db_db"] = "ezaudita_db"

from chalicelib.schema.models import User, Company, Permission
from chalicelib.new.utils.session import new_session
from chalicelib.controllers.common import CommonController
from chalicelib.controllers.user import UserController

with new_session(comment="test") as session:
    user = session.query(User).filter(User.id == 2).first()
    print(f"User email: {user.email}")
    print(f"User ID from DB: {user.id}")

    perms = session.query(Permission).filter(Permission.user_id == user.id).all()
    print(f"Perms for user ({len(perms)}): {perms}")
    for p in perms:
        print(f"  Perm company: {p.company}")
        print(f"  Perm role: {p.role}")

    companies = CommonController.get_user_companies(user, session=session)
    print(f"Companies: {companies}")

    access = UserController.get_access(user, session=session)
    print("Access dict:")
    import json
    print(json.dumps(access, indent=2))
