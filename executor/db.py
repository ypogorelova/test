from gino import Gino

DB_URI = 'postgresql://docker@docker/database/wiki_pages'

db = Gino()


class Url(db.Model):
    __tablename__ = 'urls'

    id = db.Column(db.Integer(), primary_key=True)
    chars = db.Column(db.String())
    urls = db.Column(db.JSON())


async def get_record(chars):
    """
    Select urls column from urls table for chars
    :param chars: chars to select
    :return: urls column
    """
    return await Url.select('urls').where(Url.chars == chars).gino.scalar()


async def insert_record(chars, urls):
    """
    Insert urls row to db
    :param chars: chars column
    :param urls: urls column
    :return: None
    """
    url_insert = Url(chars=chars, urls=urls)
    await url_insert.create()


async def close_db():
    """
    Close db
    :return: None
    """
    await db.pop_bind().close()


async def init_db():
    """
    Binds to database
    :return: None
    """
    await db.set_bind(DB_URI)
