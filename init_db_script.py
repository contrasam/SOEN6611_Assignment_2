from dbconnector import DBConnector
import psycopg2
		
class Init_DB():
	
	connector = None
	
	def __init__(self):
		self.connector = DBConnector.get_connection()
		self.connector.conn.autocommit = True
		
	def init_db(self):

		self.create_table_release_info()
		self.create_table_time_gap_release_versions()
		self.create_table_bug_info()
		self.create_table_reviews()
		self.create_table_summary()
	
	def create_table_release_info(self):
		cursor = self.connector.conn.cursor()
		cursor.execute('drop table IF EXISTS release_info')
		
		cursor.execute("""Create table release_info(
			os varchar(255), 
			channel varchar(255), 
			major_version int, 
			release_date date);""")
			
		cursor.close()
	
	def create_table_time_gap_release_versions(self):
		cursor = self.connector.conn.cursor()
		cursor.execute('drop table IF EXISTS release_versions_time_gap')
		cursor.execute("""Create table release_versions_time_gap(
			os varchar(255), 
			major_version int, 
			previous_major_version int, 
			release_time_gap_days int,
			release_date date,
			PRIMARY KEY (os, major_version) );""")
			
		cursor.close()
		
	def create_table_bug_info(self):
		cursor = self.connector.conn.cursor()
		cursor.execute('drop table IF EXISTS bug_info')
		cursor.execute("""Create table bug_info(
			issue_id varchar(255),
			priority int,
			milestone int,  
			status varchar(255),
			os varchar(255), 
			start_date date, 
			end_date date,
			blocked boolean,
			PRIMARY KEY (issue_id) );""")
			
		cursor.close()
	
	def create_table_reviews(self):
		cursor = self.connector.conn.cursor()
		cursor.execute('drop table IF EXISTS review_info')
		cursor.execute("""Create table review_info(
			review_id varchar(255), 
			bug_id varchar(255), 
			start_date date, 
			last_modified_date date,
			bug_blocked_time int default 0,
			PRIMARY KEY (review_id, bug_id));""")
			
		cursor.close()
		
	def create_table_summary(self):
		cursor = self.connector.conn.cursor()
		cursor.execute('drop table IF EXISTS summary_info')
		cursor.execute("""Create table summary_info(
			os varchar(255),
			release int,
			previous_release int,
			release_time_gap int,
			number_of_bugs int,
			code_review_time int,
			total_blocked_time int
		);""")
			
		cursor.close()	
		
	def create_summary_info(self):
		cursor = self.connector.conn.cursor()
		cursor.execute("""
				select release_versions_time_gap.os, major_version, previous_major_version, release_time_gap_days, count(*) as number_of_bugs, 
					DATE_PART('day', max(review_info.last_modified_date)::timestamp - min(review_info.start_date)::timestamp) as code_review_time, 
					sum(review_info.bug_blocked_time)  
				from release_versions_time_gap 
					inner join bug_info on (release_versions_time_gap.os = bug_info.os and release_versions_time_gap.major_version = bug_info.milestone) 
					inner join review_info on (bug_info.issue_id = review_info.bug_id)
				group by release_versions_time_gap.os, major_version, previous_major_version, release_time_gap_days having (release_time_gap_days > 0) 
					order by major_version desc;
			""")

		cursor_persist = self.connector.conn.cursor()

		for c in cursor.fetchall():
			cursor_persist.execute("""insert into summary_info values(%s, %s, %s, %s, %s, %s, %s)""", 
				(c[0],c[1],c[2],c[3],c[4],c[5],c[6]))	
		
		cursor_persist.close()	
		cursor.close()	
