import psycopg2
import sys

class DBConnector:

	connector = None

	def __init__(self):

		self.conn = None
		self.dbuser = None
		self.dbpassword = None
		self.dbhost = None
		self.dbname = None


	@staticmethod
	def get_connection():
		if DBConnector.connector == None:
			DBConnector.connector = DBConnector()
			DBConnector.connector.connect()

		return DBConnector.connector

	def read_conn_info(self):
		conn_info = dict()
		try:
			fo = open("conn-info.txt", "r")
			for line in fo:
				key_value = line.split('=',1)
				if len(key_value) == 2:
					conn_info[key_value[0].lstrip().rstrip()] = key_value[1].lstrip().rstrip()
		except IOError, e:
			print e

		return conn_info

	def connect(self):
		conn_info = self.read_conn_info()
		try:
			self.dbname = conn_info['dbname']
			self.dbuser = conn_info['user']
			self.dbpassword = conn_info['password']
			self.dbhost = conn_info['host']

			self.conn = psycopg2.connect(database=self.dbname,user=self.dbuser,host=self.dbhost,password=self.dbpassword)

		except:
			print sys.exc_info()[0]
			print "Unable to connect to the Database. Please check DB configuration in file conn-info.txt"


	'''def connect_db(self, dbname):
		self.dbname = dbname

		if self.dbuser is None:
			print "No user name for connection"
			return
		elif self.dbpassword is None:
			print "No password for connection"
			return
		elif self.dbhost is None:
			print "No host for connection"
			return

		try:
			self.conn = psycopg2.connect(user=self.dbuser,host=self.dbhost,password=self.dbpassword,database=self.dbname)
		except:
			print sys.exc_info()[0]
			print "Unable to connect to the Database. Please check DB configuration in file conn-info.txt"
	'''
