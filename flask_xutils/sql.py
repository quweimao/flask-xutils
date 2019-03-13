from flask import current_app
from sqlalchemy import text


class Sql(object):
    """
    This plugin is based on Flask-SQLAlchemy.
    When the class initialization need to send the SQLAlchemy Object.
    eg:
    from flask_sqlalchemy import SQLAlchemy
    db = SQLAlchemy()

    sql_object = Sql(db)
    """

    def __init__(self, db):
        self.db = db

    def fetch_value(self, query_string, bind_name='', *query_args):
        """
        Return sql origin result.
        :param query_string: The sql need to execute.
        :param bind_name: SQLAlchemy bind name.
        :param query_args: The sql extra params.

        eg:
        _sql = "SELECT * FROM table_name;"
        result = sql_object.fetch_value(_sql, bind_name="master")
        """
        if not bind_name:
            connection = self.db.get_engine(current_app).connect()
        else:
            connection = self.db.get_engine(current_app,
                                            bind=bind_name).connect()
        result = connection.execute(text(query_string), query_args)
        row = result.fetchall()
        result.close()
        connection.close()
        return row

    def fetch_dict(self, query_string, bind_name='', **query_args):
        """
        Return sql dict result.
        :param query_string: The sql need to execute.
        :param bind_name: SQLAlchemy bind name.
        :param query_args: The sql extra params.

        eg:
        _sql = "SELECT * FROM table_name;"
        result = sql_object.fetch_value(_sql, bind_name="master")
        """
        if not bind_name:
            connection = self.db.get_engine(current_app).connect()
        else:
            connection = self.db.get_engine(current_app,
                                            bind=bind_name).connect()
        result = connection.execute(text(query_string), query_args)
        rv = []
        for row in result.fetchall():
            row_dict = dict(zip(row.keys(), row))
            rv.append(row_dict)
        result.close()
        connection.close()
        return rv
