import os
import datetime
from bs4 import BeautifulSoup
from init_db_script import Init_DB
from dbconnector import DBConnector
import calendar

class Bug_Info_Extractor:

	def __init__(self):
		self.bugs_info = dict()
	
	def extract_info_fragment_1(self, bug_info_meta):
		for info_soup in bug_info_meta:
			info_key_soup = info_soup.find('th')
			
			if info_key_soup is not None:
				info_key = info_key_soup.string.split(":")[0].lstrip().rstrip()
				if info_key == 'Status':					
					info_value_soup = info_soup.find('td').find('span')
					if info_value_soup is not None:
						info_value = info_value_soup.string.lstrip().rstrip()
						self.bugs_info[info_key] = info_value	
				elif info_key == 'Closed':						
					info_value_soup = info_soup.find('td')
					if info_value_soup is not None:
						info_value = info_value_soup.string.lstrip().rstrip()
						self.bugs_info[info_key] = info_value
						
	def extract_info_fragment_2(self, bug_info_meta):
		bug_info_soup_td = bug_info_meta[0].find('td')
		
		if bug_info_soup_td is None:
			return
			
		bug_info_soup_div = bug_info_soup_td.find_all('div')
		
		if bug_info_soup_div is None:
			return
		
		for info_soup in bug_info_soup_div:
			info_key_soup_a = info_soup.find('a')
			
			if info_key_soup_a is not None:
				
				info_key_val = info_key_soup_a.text.split("-")
				
				info_key = None
				info_value = None
				
				if len(info_key_val)>=1:
					info_key = info_key_val[0].lstrip().rstrip()
					
				if len(info_key_val)>=2:
					info_value = info_key_val[1].lstrip().rstrip()
				
				if	info_key is not None:			
					self.bugs_info[info_key] = info_value
		
	def extract_info_fragment_3(self, bug_info_meta):
		for info_soup in bug_info_meta.find_all('b'):
			if 'Blocked on' in info_soup.text:
				self.bugs_info['Blocked'] = str(1)
				
	def extract_info_fragment_4(self, bug_info_meta):
		info_author_soup = bug_info_meta.find('div',class_='author')
		
		if not info_author_soup:
			return
			
		info_report_date_soup = info_author_soup.find('span',class_='date')
		
		if not info_report_date_soup:
			return 
		
		report_date_str = info_report_date_soup['title']

		date_format = "%a %b %d %H:%M:%S %Y"
		d0 = datetime.datetime.strptime(report_date_str, date_format)	
		
		info_key = 'Report_date'
		info_value = str(d0.year) + "-" + str(d0.month) + "-" + str(d0.day)
		
		self.bugs_info[info_key] = info_value

	def extract_info_fragment_5(self, bug_info_meta):
		info_header_soup = bug_info_meta.find('td', class_='vt h3')
		info_issue_link_soup = info_header_soup.find('a')
		
		info_key = 'Issue'
		info_value = info_issue_link_soup.text
			
		self.bugs_info[info_key] = info_value	
		
	def extract_bug_info(self,bug_info_soup):
		bug_meta_div = bug_info_soup.find(id='meta-float')
		
		if bug_meta_div is None:
			return
			
		bug_meta_table = bug_meta_div.find('table')
		
		if bug_meta_table is None:
			return
			
		bug_meta_info_soup = bug_meta_table.find_all('tr');
		
		if bug_meta_info_soup is None:
			return
		
		bug_info_frag_1 = bug_meta_info_soup[:-1]
		bug_info_frag_2 = bug_meta_info_soup[-1:]
		
		self.extract_info_fragment_1(bug_info_frag_1)
		self.extract_info_fragment_2(bug_info_frag_2)
		
		bug_info_frag_3 = bug_meta_div.find('div',class_='rel_issues')
		
		if bug_info_frag_3 is not None:
			self.extract_info_fragment_3(bug_info_frag_3)
			
		bug_info_frag_4 = bug_info_soup.find(id='hc0')
		
		self.extract_info_fragment_4(bug_info_frag_4)
		
		bug_info_frag_5 = bug_info_soup.find(id='issueheader')
		
		self.extract_info_fragment_5(bug_info_frag_5)
		
		self.save_bug_info()
		
	def format_end_date(self, end_date_str, start_date_str):
	
		d0 = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
		end_date = None
		
		if end_date_str == 'Yesterday':
			end_date = datetime.datetime(d0.year,d0.month,d0.day)
			day_difference = datetime.timedelta(days=-1)
			end_date = end_date + day_difference
			return end_date
		elif end_date_str == 'Today':
			end_date = datetime.datetime(d0.year,d0.month,d0.day)
			return end_date
			
		try:
			date_day_or_year = int(end_date_str.split()[1].lstrip().rstrip())
			if date_day_or_year <= 31:
				d1 = datetime.datetime.strptime(end_date_str, "%b %d")
				diff_month = d0.month - d1.month
				if diff_month <= 0:
					end_date = datetime.datetime(d0.year,d1.month,d1.day)
				else:
					end_date = datetime.datetime(d0.year + 1,d1.month,d1.day)
			else:
				d1 = datetime.datetime.strptime(end_date_str, "%b %Y")
				end_date = datetime.datetime(d1.year,d1.month,calendar.monthrange(d1.year, d1.month)[1])
				
			return end_date	
		except:
			print end_date_str
		
	def save_bug_info(self):
		connector = DBConnector.get_connection()
		connector.conn.autocommit = True
		cursor = connector.conn.cursor()
		
		issue_id = self.bugs_info.get('Issue', None)
		
		priority = self.bugs_info.get('Pri', None)
		try:
			priority = int(priority)
		except:
			priority = None
			
		milestone = self.bugs_info.get('M', None)
		try:
			milestone = int(milestone)
		except:
			milestone = None
			
		status = self.bugs_info.get('Status', None)
		os = self.bugs_info.get('OS', None)
		
		if os is not None:
			if 'Win7' in os:
				os = 'Windows'
				
		start_date = self.bugs_info.get('Report_date', None)
		end_date = self.bugs_info.get('Closed', None)
		
		if end_date is not None and start_date is not None:
			end_date = self.format_end_date(end_date.lstrip().rstrip(), start_date)
			end_date = str(end_date.year) + "-" + str(end_date.month) + "-" + str(end_date.day)
		
		blocked = self.bugs_info.get('Blocked', None)
		
		if (milestone is not None) and (os is not None) and (not 'All' in os):
			cursor.execute("""
				insert into bug_info values(%s, %s, %s, %s, %s, %s, %s, %s)
			""",
			(issue_id,priority,milestone,status,os,start_date,end_date,blocked))
		
		
		del self.bugs_info
		self.bugs_info = dict()	
		cursor.close()
	
	def extract_bug_info_dump(self):
		bugs_path = './raw_chrome_issue'
		
		for f in os.listdir(bugs_path):	
			if os.path.isfile(os.path.join(bugs_path,f)):
				try:
					bug_info_html = open(os.path.join(bugs_path,f), "r")
					bug_info_soup = BeautifulSoup(bug_info_html.read())
					
					if bug_info_soup is not None:
						self.extract_bug_info(bug_info_soup)
						
				except IOError as e:
					print e

	def calculate_bug_blocked_time(self):
		
		connector = DBConnector.get_connection()
		connector.conn.autocommit = True
		cursor_fetch = connector.conn.cursor()
		
		cursor_fetch.execute("""
			select bug_info.issue_id, review_info.last_modified_date as review_end_date, 
					bug_info.end_date as bug_end_date, review_info.review_id from review_info inner join bug_info 
						on review_info.bug_id = bug_info.issue_id where bug_info.blocked = 't' and bug_info.end_date is not null
							and bug_info.end_date > review_info.last_modified_date order by review_end_date asc
		""")				
				
		cursor_persist = connector.conn.cursor()
		
		result_set = cursor_fetch.fetchall()

		#for m in result_set:
		#	print m	

		result_set_size = len(result_set)

		block_time_tuples = []

		start_x = 0
		start_y = 0

		for x in range(start_x, result_set_size):
			bug_id_x = result_set[x][0]
			review_end_date_x = result_set[x][1]
			bug_end_date_x = result_set[x][2]
			review_id_x = result_set[x][3]

			for y in range(start_y, result_set_size):
				bug_id_y = result_set[y][0]
				review_end_date_y = result_set[y][1]
				bug_end_date_y = result_set[y][2]
				review_id_y = result_set[y][3]

				#print str(x) + ":" + str(y)

				if review_id_x != review_id_y and review_end_date_y >= review_end_date_x:
					if 	review_end_date_y <= bug_end_date_x and bug_end_date_y <= bug_end_date_x:
						continue
					else:
						block_time_tuples.append((bug_id_x,review_end_date_x,bug_end_date_x, review_id_x))
						#print review_id_x + ":" + review_id_y + ":" + str(review_end_date_y) + ":" + ":" + str(bug_end_date_y) + ":" + str(review_end_date_x) + ":" + str(bug_end_date_x) 
						start_x = y
						start_y = y
						break

				if x == (result_set_size - 1):
					block_time_tuples.append((bug_id_y,review_end_date_y,bug_end_date_y,review_id_y))


		result_set_size = len(block_time_tuples) 
					
		for x in range(0, result_set_size):
			bug_id_x = block_time_tuples[x][0]
			review_end_date_x = block_time_tuples[x][1]
			bug_end_date_x = block_time_tuples[x][2]
			review_id_x = block_time_tuples[x][3]

			y = None
			p = None

			block_time = 0
			
			if x == (result_set_size - 1):
				p = x - 1
				bug_id_p = block_time_tuples[p][0]
				review_end_date_p = block_time_tuples[p][1]
				bug_end_date_p = block_time_tuples[p][2]
				review_id_p = block_time_tuples[p][3]
			else:
				y = x + 1
				bug_id_y = block_time_tuples[y][0]
				review_end_date_y = block_time_tuples[y][1]
				bug_end_date_y = block_time_tuples[y][2]
				review_id_y = block_time_tuples[y][3]

			if x == (result_set_size - 1):
				if review_id_x != review_id_p and review_end_date_x >= review_end_date_p:
					block_time = (bug_end_date_x - review_end_date_x).days
					
			else:		
				if review_id_x != review_id_y and review_end_date_x <= review_end_date_y:
						if review_end_date_y >= bug_end_date_x:
							block_time = (bug_end_date_x - review_end_date_x).days
						elif bug_end_date_y >= bug_end_date_x:
							block_time = (bug_end_date_x - review_end_date_x).days - (bug_end_date_x - review_end_date_y).days
						

			cursor_persist.execute("""
				update review_info set bug_blocked_time = %s where review_id = %s and bug_id = %s 
			""", (block_time,review_id_x,bug_id_x))				
		
		cursor_persist.close()
		cursor_fetch.close()