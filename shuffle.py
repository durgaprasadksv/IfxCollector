import os
from datetime import datetime

log_stat_dic = {}

#	root:	the directory to be scan 
#	return:	list of paths of all syslog files
def scan_syslog_dir(userlog_root):
	syslog_path_list = []
	app_dir_list = os.listdir(userlog_root)
	for app_dir in app_dir_list:
		syslog_file_list = os.listdir(userlog_root + "/" + app_dir)
		for syslog_file in syslog_file_list:
			syslog_path_list.append(userlog_root + "/" + app_dir + "/" + syslog_file + "/" + "syslog");
	return syslog_path_list
	
#	line:	the text
# 	return:	True if this line has the shuffle stat data
# 	return:	False not
def is_shuffle_stat(line):
	if ("org.apache.hadoop.mapreduce.task.reduce.Fetcher:" in line) and ("about to shuffle output of map" in line):
		return True
	return False

#	line:	the text
#	return:	the object datetime, data size in bytes, source Map task ID,  
def get_shuffle_stat(line):
	token = line.split(" ")
	token_num = len(token)
	timestamp = datetime.strptime(token[0] + " " + token[1], "%Y-%m-%d %H:%M:%S,%f")
	size = token[token_num - 5]
	map_ID = token[token_num - 7].split("_")[-2]
	return timestamp, size, int(map_ID)


def is_reducer_finshed(line):
	if ("org.apache.hadoop.mapred.Task:" in line) and ("done" in line):
		return True
	return False

# return job ID and task ID
def get_ID(path):
	token = path.split("/")
	token = token[-2].split("_")
	return int(token[2]), int(token[4]) 

# scan the log directory
# create a map for every log container, mark lines it has scanned and if it is done
# check if it is done, if so, skip this log file
# if not, go to the current lines
# report the amount of shuffle data
def shuffle():
	syslog_path_list = scan_syslog_dir("/mnt/var/log/hadoop/userlogs")
	for syslog in syslog_path_list:
		if syslog not in log_stat_dic:
			stat_list = []
			# is it finished
			stat_list.append(False)
			# which line should be read next
			stat_list.append(0)
			log_stat_dic[syslog] = stat_list
		# read the log
		finshed = log_stat_dic[syslog][0]
		if finshed == True:
			break

		job_ID, task_ID = get_ID(syslog)
		start_line = log_stat_dic[syslog][1]
		log_file = open(syslog)
		lines = log_file.readlines()
		log_file.close()

		for line_index in range(start_line, len(lines)):
			line = lines[line_index]
			if is_reducer_finshed(line) == True:
				# set the status
				log_stat_dic[syslog][0] = True
			else:
				if is_shuffle_stat(line) == True:
					timestamp, size, map_ID = get_shuffle_stat(line);
					print timestamp, "job ", job_ID, " Reducer ", task_ID , \
						" data size " , size, " from Mapper", map_ID
		log_stat_dic[syslog][1] = len(lines)


# shuffle()
# "http://54.175.58.152:8086", "root", "1234567"
# ip
# port
# user
# passport
# seriesName
# def send_shuffle():
	# a = 1

# /mnt/var/log/hadoop/userlogs


# strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
# 'Thu, 28 Jun 2001 14:17:15 +0000'

# print scan_syslog_dir("/home/pengcheng/app")