import sqlalchemy
from chalice import ChaliceViewError

from chalicelib.__version__ import __version__
from chalicelib.blueprints.superblueprint import SuperBlueprint
from chalicelib.schema import engine

bp = SuperBlueprint(__name__)


@bp.route("/health/api")
def health():
    return {"status": "ok"}


@bp.route("/health/db")
def health_db():
    try:
        conn = engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        raise ChaliceViewError(e) from e
    else:
        conn.close()
        return {"status": "ok"}


@bp.route("/version")
def version():
    return {"version": __version__}
