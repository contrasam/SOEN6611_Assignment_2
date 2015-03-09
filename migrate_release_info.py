import json
import sys
from pprint import pprint
import psycopg2
import time
from datetime import datetime
from dbconnector import DBConnector
from init_db_script import Init_DB

class Release_Info:
	
	def migrate(self):
		release_info = self.read_release_info_json()
		self.insert_into_db(release_info)
		
	def measure_time_gap(self):
		distinct_os = self.fetch_all_os()
		distinct_os_major_versions = self.fetch_all_major_versions(distinct_os)
		self.calculate_and_save_time_gap(distinct_os_major_versions)
	
	def calculate_and_save_time_gap(self, distinct_os_major_versions):
		
		connector = DBConnector.get_connection()
		connector.conn.autocommit = True
		cursor = connector.conn.cursor()
		 
		for os, versions in distinct_os_major_versions.iteritems():
			time_gap_versions = []
			
			for x in versions:
				for y in versions:
					if x != y and x > y and (x - y) == 1:
						time_gap_versions.append((x, y))

			for version_tuple in time_gap_versions:
				release_dates = []

				v1 = version_tuple[0]
				v2 = version_tuple[1]

				sql = "select MAX(release_date) from release_info where os='{0}' and major_version={1} group by os, major_version;".format(os, v1)
				cursor.execute(sql)
				for rl_date in cursor.fetchall():
					release_dates.append(rl_date[0])

				sql = "select MIN(release_date) from release_info where os='{0}' and major_version={1} group by os, major_version;".format(os, v2)
				cursor.execute(sql)
				for rl_date in cursor.fetchall():
					release_dates.append(rl_date[0])				
					
				
				date_format = "%Y-%m-%d"
				d0 = datetime.strptime(str(release_dates[0]), date_format)
				d1 = datetime.strptime(str(release_dates[1]), date_format)
				delta = d0 - d1
				
				time_gap = delta.days 
				
				self.save_release_version_time_gap(cursor,os,version_tuple[0],version_tuple[1],time_gap,release_dates[0])
				
		cursor.close()
		
		
	def save_release_version_time_gap(self, cursor, os, major_version, previous_major_version, time_gap, release_date):
		sql = "insert into release_versions_time_gap values('{0}', {1}, {2}, {3}, '{4}')".format(os, major_version,previous_major_version,time_gap,release_date)
		cursor.execute(sql)
		
	def fetch_all_major_versions(self, distinct_os):
		connector = DBConnector.get_connection()
		connector.conn.autocommit = True
		cursor = connector.conn.cursor()
		
		distinct_os_major_versions = {}
		
		for os in distinct_os:
			sql = "select distinct major_version from release_info where os = '{0}' order by major_version desc;".format(os)	
			cursor.execute(sql)
			distinct_os_major_versions[os] = []
			
			for version in cursor.fetchall():
				distinct_os_major_versions[os].append(version[0])
		
		
		cursor.close()
		return distinct_os_major_versions
		
		
	def fetch_all_os(self):
		connector = DBConnector.get_connection()
		connector.conn.autocommit = True
		
		sql = "select distinct os from release_info;"
		cursor = connector.conn.cursor()
		cursor.execute(sql)
		
		distinct_os = []
		
		for os in cursor.fetchall():
			distinct_os.append(os[0])
		
		cursor.close()
		
		return distinct_os
	
	def filter_os_windows(self,info):
		#return 'Windows' in info['os']
		return True
			
		
	def insert_into_db(self, release_info):
		connector = DBConnector.get_connection()
		connector.conn.autocommit = True
		cursor = connector.conn.cursor()
		
		release_info = filter(self.filter_os_windows,release_info)
		
		for info in release_info:
			os = info['os']
			channel = info['channel']
			version = info['version']
			release_date = info['timestamp'].split()[0]
			
			major_version = (int)(version.split('.')[0])
			
			sql = "insert into release_info values('{0}', '{1}', {2}, '{3}')".format(os, channel,major_version,release_date)
			cursor.execute(sql)
		
		
		connector.conn.commit()		
		cursor.close()
			
	def read_release_info_json(self):
		
		json_data = open('release-info.json')

		release_info = json.load(json_data)
		
		json_data.close()
		
		return release_info
		


