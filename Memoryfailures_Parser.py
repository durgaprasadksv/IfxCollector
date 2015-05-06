#!/usr/bin/env python

#This parser will help users to indentify memory failure due to which mapper on which node.
#Note: please fill your memory failure types in the function parser_history_log(mfile)

import sys
import os
import re
import time
import threading
from optparse import OptionParser
from util import *

mapper_memory_failuers = {}

def parser_history_log(mfile):
	context = {}
	m_file = open(mfile)
	for line in m_file:
		if ('MAP_ATTEMPT_FAILED' in line) and ('"error":"Error: Java heap space"' in line):
			context['TaskID'] = get_TaskID(line)
			context['ErrorMsg'] = get_ErrorMsg(line)
			context['AttemptID'] = get_AttemptID(line)
			context['HostName'] = get_HostName(line)
			context['ContainerId'] = get_ContainerId(m_file, context['TaskID'])		
			print context
	return context


def get_TaskID(line):
	token = line.split(",")
	token = token[1].split(":")
	return token[3]

def get_AttemptID(line):
	token = line.split(",")
	token = token[3].split(":")
	return token[1]

def get_ContainerId(mfile, TaskID):
	for line in mfile:
		if ('MAP_ATTEMPT_STARTED' in line) and (TaskID in line):
			token = line.split(",")
			token = token[8].split(":")
			return token[1]
	return ''

def get_HostName(line):
	token = line.split(",")
	token = token[5].split(":")
	return token[1]

def get_ErrorMsg(line):
	token = line.split(",")
	token = token[9].split(":")
	return  token[2]


def Parser(path):
	files = os.listdir(path)
	for f in files:
		if f.endswith('jhist'):
			data = parser_history_log(path + '/' + f)
			mapper_memory_failuers[f] = data


# Note: when calling Parse(path) function, please make the directory of logs as the parameter.
# Parser("/mnt/var/log/hadoop/history/2015/05/05/000000")
#