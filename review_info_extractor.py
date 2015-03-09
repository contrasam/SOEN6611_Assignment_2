import re
import datetime
from init_db_script import Init_DB
from bs4 import BeautifulSoup
import urllib2
from dbconnector import DBConnector
from os import walk
import time
import random
import codecs
import os


class Review_Info_Extractor:

    def get_time(self,info_soup,field):
        for soup_div in info_soup.find_all('div'):
            is_field = (soup_div.find('b').text == field)
            if is_field:
                field_value = soup_div.text.split(':')[1].strip()

                if field_value is not None:
                    today = datetime.datetime.today()

                    regex_match = re.search(r'(\d)\syear(s?)', field_value, re.DOTALL)

                    if regex_match is not None:
                        year = regex_match.group(1)
                        total_days = (int(year) * 365)
                        delta = datetime.timedelta(days=-int(total_days))
                        today = today + delta

                    regex_match = re.search(r'(\d*)\smonth(s?)', field_value, re.DOTALL)

                    if regex_match is not None:
                        months = regex_match.group(1)
                        total_days = (int(months) * 30)
                        delta = datetime.timedelta(days=-int(total_days))
                        today = today + delta

                    regex_match = re.search(r'(\d*)\sweek(s?)', field_value, re.DOTALL)

                    if regex_match is not None:
                        weeks = regex_match.group(1)
                        total_days = (int(weeks) * 7)
                        delta = datetime.timedelta(days=-int(total_days))
                        today = today + delta    

                    regex_match = re.search(r'(\d*)\sday(s?)', field_value, re.DOTALL)

                    if regex_match is not None:
                        days = regex_match.group(1)
                        delta = datetime.timedelta(days=-int(days))
                        today = today + delta    

                    regex_match = re.search(r'(\d*)\shours(s?)', field_value, re.DOTALL)

                    if regex_match is not None:
                        hours = regex_match.group(1)
                        delta = datetime.timedelta(hours=-int(hours))
                        today = today + delta

                    regex_match = re.search(r'(\d*)\sminute(s?)', field_value, re.DOTALL)

                    if regex_match is not None:
                        minutes = regex_match.group(1)
                        delta = datetime.timedelta(minutes=-int(minutes))
                        today = today + delta        
                    
                    return today
        return None            

    def extract_reviews_info(self):
        connector = DBConnector.get_connection()
        connector.conn.autocommit = True
        cursor = connector.conn.cursor()

        files = []
        for (dirpath, dirnames, filenames) in walk("./raw_chrome_review"):
            files.extend(filenames)
            break

        for file in files:
            with open('./raw_chrome_review/' + str(file), 'r') as content_file:
                review_id = file.split(".")[0]
                content = content_file.read()
                soup = BeautifulSoup(content)
                title = ""
                reviewText = ""
                sidebarText = ""
                createdTimeFormat = ""
                modifiedTimeFormat = ""
                today = datetime.datetime.today()
                createdTime = None
                modifiedTime = None
                createDelta = None
                modifiedDelta = None
                bug_array = []


                soup_div = soup.find(id='issue-description')

                for soup_a in soup_div.find_all('a'):
                    match = re.search(r'http://code.google.com/p/chromium/issues/detail',soup_a['href'])
                    if match:
                        bug_array.append(soup_a.text)

                for div in soup.find('div', {"class": "issue_details_sidebar"}):
                    contents = div.encode('utf-8')
                    sidebarText += contents

                issue_meta_div_soup = soup.find('div', {"class": "issue_details_sidebar"})    
                created_time = self.get_time(issue_meta_div_soup, 'Created:') 
                modified_time = self.get_time(issue_meta_div_soup, 'Modified:')

                bug_array = set(bug_array)

                for bug in bug_array:

                    if bug.strip() != "":
                        
                        
                        if created_time is not None:
                            created_time_str = str(created_time.date())
                        if modified_time is not None:
                            modified_time_str = str(modified_time.date())
                           
                        cursor.execute("""
                            insert into review_info values(%s, %s, %s, %s)
                        """,(review_id.strip(), bug.strip(), created_time_str, modified_time_str))

        cursor.close()

    def fix_review_end_time(self):
        connector = DBConnector.get_connection()
        connector.conn.autocommit = True
        cursor_fetch = connector.conn.cursor()

        cursor_fetch.execute(""" 
            select bug_info.issue_id, review_info.review_id, bug_info.end_date, review_info.start_date,
                review_info.last_modified_date
            from review_info inner join bug_info on review_info.bug_id = bug_info.issue_id  
        """)

        cursor_persist = connector.conn.cursor()

        for c in cursor_fetch.fetchall():
            bug_id = c[0]
            review_id = c[1]
            bug_end_date = c[2]
            review_start_date = c[3]
            review_end_date = c[4]

            if bug_end_date is not None and review_end_date > bug_end_date:

                if bug_end_date<review_start_date:
                    bug_end_date = review_start_date

                cursor_persist.execute(""" 
                    update review_info set last_modified_date = %s where review_id = %s and bug_id = %s
                """,(bug_end_date, review_id, bug_id))

        cursor_persist.close()
        cursor_fetch.close()                





  
    