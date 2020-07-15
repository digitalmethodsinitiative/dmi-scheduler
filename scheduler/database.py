"""
Database wrapper
"""
import psycopg2.extras
import psycopg2

from psycopg2 import sql
from psycopg2.extras import execute_values


class Database:
	"""
	Simple database handler

	Offers a number of abstraction methods that limit how much SQL one is
	required to write. Also makes the database connection mostly multithreading
	proof by instantiating a new cursor for each query (and closing it afterwards)
	"""
	_cursor = None
	_log = None

	def __init__(self, logger, dbname, user, password, host, port, appname=""):
		"""
		Set up database connection

		:param logger:  Logger instance
		:param dbname:  Database name
		:param user:  Database username
		:param password:  Database password
		:param host:  Database server address
		:param port:  Database port
		:param appname:  App name, mostly useful to trace connections in pg_stat_activity
		"""

		appname = "dmi-db" if not appname else "dmi-db-%s" % appname

		self.connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port,
										   application_name=appname)
		self._cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		self._log = logger

		if self._log is None:
			raise NotImplementedError()

		self.commit()

	def query(self, query, replacements=None, cursor=None):
		"""
		Execute a query

		:param string query: Query
		:param args: Replacement values
		:param cursor: Cursor to use. Default - use common cursor
		:return None:
		"""
		if not cursor:
			cursor = self.get_cursor()

		self._log.debug("Executing query %s" % self._cursor.mogrify(query, replacements))

		return cursor.execute(query, replacements)

	def execute(self, query, replacements=None):
		"""
		Execute a query, and commit afterwards

		This is required for UPDATE/INSERT/DELETE/etc to stick
		:param string query:  Query
		:param replacements: Replacement values
		"""
		cursor = self.get_cursor()

		self._log.debug("Executing query %s" % self._cursor.mogrify(query, replacements))
		cursor.execute(query, replacements)
		self.commit()

		cursor.close()

	def execute_many(self, query, replacements=None):
		"""
		Execute a query multiple times, each time with different values

		This makes it particularly suitable for INSERT queries, but other types
		of query using VALUES are possible too.

		:param string query:  Query
		:param replacements: A list of replacement values
		"""
		cursor = self.get_cursor()
		execute_values(cursor, query, replacements)
		cursor.close()

	def update(self, table, data, where=None, commit=True):
		"""
		Update a database record

		:param string table:  Table to update
		:param dict where:  Simple conditions, parsed as "column1 = value1 AND column2 = value2" etc
		:param dict data:  Data to set, Column => Value
		:param bool commit:  Whether to commit after executing the query

		:return int: Number of affected rows. Note that this may be unreliable if `commit` is `False`
		"""
		if where is None:
			where = {}

		# build query
		identifiers = [sql.Identifier(column) for column in data.keys()]
		identifiers.insert(0, sql.Identifier(table))
		replacements = list(data.values())

		query = "UPDATE {} SET " + ", ".join(["{} = %s" for column in data])
		if where:
			query += " WHERE " + " AND ".join(["{} = %s" for column in where])
			for column in where.keys():
				identifiers.append(sql.Identifier(column))
				replacements.append(where[column])

		query = sql.SQL(query).format(*identifiers)

		cursor = self.get_cursor()
		self._log.debug("Executing query: %s" % cursor.mogrify(query, replacements))
		cursor.execute(query, replacements)

		if commit:
			self.commit()

		result = cursor.rowcount
		cursor.close()
		return result

	def delete(self, table, where, commit=True):
		"""
		Delete a database record

		:param string table:  Table to delete from
		:param dict where:  Simple conditions, parsed as "column1 = value1 AND column2 = value2" etc
		:param bool commit:  Whether to commit after executing the query

		:return int: Number of affected rows. Note that this may be unreliable if `commit` is `False`
		"""
		where_sql = ["{} = %s" for column in where.keys()]
		replacements = list(where.values())

		# build query
		identifiers = [sql.Identifier(column) for column in where.keys()]
		identifiers.insert(0, sql.Identifier(table))
		query = sql.SQL("DELETE FROM {} WHERE " + " AND ".join(where_sql)).format(*identifiers)

		cursor = self.get_cursor()
		self._log.debug("Executing query: %s" % cursor.mogrify(query, replacements))
		cursor.execute(query, replacements)

		if commit:
			self.commit()

		result = cursor.rowcount
		cursor.close()
		return result

	def insert(self, table, data, commit=True, safe=False, constraints=None):
		"""
		Create database record

		:param string table:  Table to insert record into
		:param dict data:   Data to insert
		:param bool commit: Whether to commit after executing the query
		:param bool safe: If set to `True`, "ON CONFLICT DO NOTHING" is added to the insert query, so that no error is
						  thrown when the insert violates a unique index or other constraint
		:param tuple constraints: If `safe` is `True`, this tuple may contain the columns that should be used as a
								  constraint, e.g. ON CONFLICT (name, lastname) DO NOTHING
		:return int: Number of affected rows. Note that this may be unreliable if `commit` is `False`
		"""
		if constraints is None:
			constraints = []

		# escape identifiers
		identifiers = [sql.Identifier(column) for column in data.keys()]
		identifiers.insert(0, sql.Identifier(table))

		# construct ON NOTHING bit of query
		if safe:
			safe_bit = " ON CONFLICT "
			if constraints:
				safe_bit += "(" + ", ".join(["{}" for each in constraints]) + ")"
				for column in constraints:
					identifiers.append(sql.Identifier(column))
			safe_bit += " DO NOTHING"
		else:
			safe_bit = ""

		# prepare parameter replacements
		protoquery = "INSERT INTO {} (%s) VALUES %%s" % ", ".join(["{}" for column in data.keys()]) + safe_bit
		query = sql.SQL(protoquery).format(*identifiers)
		replacements = (tuple(data.values()),)

		cursor = self.get_cursor()
		self._log.debug("Executing query: %s" % cursor.mogrify(query, replacements))
		cursor.execute(query, replacements)

		if commit:
			self.commit()

		result = cursor.rowcount
		cursor.close()
		return result

	def fetchall(self, query, *args):
		"""
		Fetch all rows for a query

		:param string query:  Query
		:param args: Replacement values
		:return list: The result rows, as a list
		"""
		cursor = self.get_cursor()
		self._log.debug("Executing query: %s" % cursor.mogrify(query, *args))
		self.query(query, cursor=cursor, *args)

		try:
			result = cursor.fetchall()
		except AttributeError:
			result = []

		cursor.close()
		return result

	def fetchone(self, query, *args):
		"""
		Fetch one result row

		:param string query: Query
		:param args: Replacement values
		:return: The row, as a dictionary, or None if there were no rows
		"""
		cursor = self.get_cursor()
		self.query(query, cursor=cursor, *args)

		try:
			result = cursor.fetchone()
		except psycopg2.ProgrammingError as e:
			# no results to fetch
			self.commit()
			result = None

		cursor.close()
		return result

	def commit(self):
		"""
		Commit the current transaction

		This is required for UPDATE etc to stick.
		"""
		self.connection.commit()

	def rollback(self):
		"""
		Roll back the current transaction
		"""
		self.connection.rollback()

	def close(self):
		"""
		Close connection

		Running queries after this is probably a bad idea!
		"""
		self.connection.close()

	def get_cursor(self):
		"""
		Get a new cursor

		Re-using cursors seems to give issues when using per-thread
		connections, so simply instantiate a new one each time

		:return: Cursor
		"""
		return self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
