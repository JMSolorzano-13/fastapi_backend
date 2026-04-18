import xml.etree.ElementTree as etree

import sqlalchemy


class XMLType(sqlalchemy.types.UserDefinedType):
    cache_ok = True

    def get_col_spec(self):
        return "XML"

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            if isinstance(value, str):
                return value
            else:
                return etree.tostring(value, encoding="unicode")

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value

        return process
