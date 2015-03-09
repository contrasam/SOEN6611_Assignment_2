from init_db_script import Init_DB
from bug_info_extractor import Bug_Info_Extractor
from migrate_release_info import Release_Info
from review_info_extractor import Review_Info_Extractor

class Main_Script:
	
	def start_here(self):

		print "\nInitializing and Preparing Database...\n"
		init_script = Init_DB()
		init_script.init_db()
		print "\nDatabase initialized successfully...\n"
		
		print "\nExtracting and Migrating Chrome Release information into Database...\n"
		ri = Release_Info()
		ri.migrate()
		print "\nMigrated Release information successfully...\n"
		
		print "\nCalculating the Saving the time gap between milestone releases...\n"
		ri.measure_time_gap()
		print "\nCalculated and Saved time gap between releases successfully...\n"
		
		print "\nExtracting and Migrating Code Review information into Database...\n"
		review_info_extractor = Review_Info_Extractor()
		review_info_extractor.extract_reviews_info()
		print "\nCode Review information successfully extracted and saved into Database...\n"
		
		print "\nExtracting and Migrating Bug Information into Database... This may take time...\n"
		bug_info_extractor = Bug_Info_Extractor()
		bug_info_extractor.extract_bug_info_dump()
		bug_info_extractor.calculate_bug_blocked_time()
		print "\nBug information successfully extracted and saved into Database...\n"
	
		review_info_extractor = Review_Info_Extractor()
		review_info_extractor.fix_review_end_time()
		
		print "\nCreate summary information...\n"
		init_script.create_summary_info()
		print "\nCreated and Saved summary information...\n"
		
		
Main_Script().start_here()		
		

		