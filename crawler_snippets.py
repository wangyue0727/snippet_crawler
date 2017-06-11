# -*- coding: utf-8 -*-
import sys,os
import re
import urllib
import urllib2
import codecs
import time
import math
import string
from datetime import date, timedelta, datetime
from bs4 import BeautifulSoup
import json
import smtplib
from email.mime.text import MIMEText
import requests
import argparse

import traceback

reload(sys)
sys.setdefaultencoding('utf-8')

class snippets_crawler():
	def __init__(self, query_id, query):
		self.query_id = query_id
		self.query = query
		self.results_limit = 100
		self.pages_cnt = 1
		self.crawl_idx = 1
		self.url_base = ''
		self.parameters = {}
		self.url_list = []
		self.results = []
		self.output_root = os.path.join('./snippets/', str(query_id))
		
		if not os.path.exists(self.output_root):
			os.makedirs(self.output_root)				

		
	def get_page(self,url,para=None):
		try:
			response = requests.get(url, params=para)
			wait = 60
			if response.status_code == 403:
				print '403 ' + url
				sys.exit()
			while response.status_code != 200:
				print '	Wrong response code (' + str(response.status_code) + ") when crawling: " + response.url
				with codecs.open('./error.html', 'wb', 'utf-8') as out:
					out.write(response.text)  
				print '	Wait '+ str(wait/60) + ' mins to retry'
				time.sleep(wait)
				wait+=wait
				response = requests.get(url, params=para)

			time.sleep(2)
			response.encoding = 'utf-8'
			return response.url,response.text

		except Exception as e:
			print "[Error] There is an error: "
			print traceback.print_exc()	 

		# except:
			print 'Error: ' + url
			return 'ERROR','ERROR'			   

	def google_crawl(self):
		self.url_base = 'https://www.google.com/search?'
		self.parameters = {}
		self.parameters['q'] = self.query
		self.parameters['hl'] = 'en'

		self.pages_cnt = 1
		self.crawl_idx = 1
		self.results = []
		self.url_list = []

		print '  Crawling Google data...'
		
		final_url, content = self.get_page(self.url_base, self.parameters)
	
		with codecs.open('./google_test', 'wb', 'utf-8') as out:
			out.write(content)	   
		self.google_get_search_results(final_url, content)
		with codecs.open(os.path.join(self.output_root, 'google.json'), 'wb', 'utf-8') as f:
			json.dump(self.results, f, indent=4)

	def google_get_search_results(self, url, content):
		page = BeautifulSoup(content, 'lxml')
		
		if page.find('div', id='ires'):
			#results_cnt = int(page.find(id='sortBy').find('span', 'sortRight').span.string.split()[-2])
			list_results = page.find('div', id='ires').find_all('div', 'g')			
			for l in list_results:
				if l.find('h3', 'r'):
					if not l.find('h3','r').find('a'):
						continue
					href = l.find('h3', 'r').a['href']
					# print href
					decoder = href.split('?')[1].split('&')
					#print decoder
					href_link = ''
					for d in decoder:
						#print d
						if d.split('=')[0] == 'q':
							href_link = d.split('=')[1]
							if len(href_link) > 0:
								url = href_link
								#if url not in self.url_list:
								self.url_list.append(url)
								if l.find('span', 'st'):
									snippets = l.find('span', 'st').get_text()
								else:
									snippets = u''
								# print snippets
								self.results.append({'id':self.query_id+'_google_'+str(self.crawl_idx), 'url':url, 'snippets':snippets})
				
								self.crawl_idx += 1
								#print self.pages_cnt,self.max_pages
								if self.crawl_idx > self.results_limit:
									self.crawl_idx = 1
									return								

			if page.find('table', id='nav'):
				page_index = page.find('table', id='nav').find_all('td')
				
				#for ele in page_index:
					#print ele
					   
				for i in range(len(page_index)):
					if not page_index[i].find('a'):
						if self.pages_cnt == 1:
							next_page = page_index[i+2]
						else:
							next_page = page_index[i+1]
						
						if next_page.find('a'):
							next_url = 'https://www.google.com'+next_page.a['href']
							self.pages_cnt += 1					
							next_url, next_content = self.get_page(next_url)
							self.google_get_search_results(next_url, next_content)
						else:
							return
						break

	def bing_crawl_new(self):
		self.url_base = 'https://www.bing.com/search?'
		self.parameters = {}
		self.parameters['q'] = self.query

		self.crawl_idx = 1
		self.results = []
		self.url_list = []
 
		print '  Crawling Bing data...'
		final_url, content = self.get_page(self.url_base, self.parameters)
		
		with codecs.open('./bing_test', 'wb', 'utf-8') as out:
			out.write(content)  

		self.bing_get_search_results(final_url, content)
		with codecs.open(os.path.join(self.output_root, 'bing.json'),'wb','utf-8') as f:
			json.dump(self.results, f, indent=4)   

	def bing_get_search_results(self, url, content):
		page = BeautifulSoup(content, 'lxml')

		if page.find('ol', id='b_results'):
			list_results = page.find('ol', id='b_results').find_all('li','b_algo')
			# print len(list_results)
			for l in list_results:
				if l.find('h2'):
					href_link = l.find('h2').a['href']
					if len(href_link) > 0:
						url = href_link
						if url not in self.url_list:
							self.url_list.append(url)
							if l.find('p'):
								snippets = l.find('p').get_text()
							else:
								snippets = u''

							self.results.append({'id':self.query_id+'_bing_'+str(self.crawl_idx), 'url':url, 'snippets':snippets})
			
							self.crawl_idx += 1
							#print self.pages_cnt,self.max_pages
							if self.crawl_idx > self.results_limit:
								self.crawl_idx = 1
								return
			
			if page.find('ul', {"aria-label":"More pages with results"}):
				
				# print page.find('ul', {"aria-label":"More pages with results"})
				if page.find('ul', {"aria-label":"More pages with results"}).find('a','sb_pagS'):
					current_page = page.find('ul', {"aria-label":"More pages with results"}).find('a','sb_pagS').get_text()
					next_page = str(int(current_page)+1)
					# print current_page+"\t"+next_page
					next_page = re.compile(next_page)

					if page.find('ul', {"aria-label":"More pages with results"}).find('a',text=next_page):
						# print page.find('ul', {"aria-label":"More pages with results"}).find('a',text=next_page)['href']
						next_url = "https://www.bing.com" + page.find('ul', {"aria-label":"More pages with results"}).find('a',text=next_page)['href']	   
						next_url, next_content = self.get_page(next_url)
						# print next_url
						self.bing_get_search_results(next_url, next_content)
					else:
						return

	def yahoo_crawl(self):
		self.url_base = 'http://search.yahoo.com/search?'
		self.parameters = {}
		self.parameters['p'] = self.query

		self.crawl_idx = 1
		self.results = []
		self.url_list = []
		final_url, content = self.get_page(self.url_base, self.parameters)
		
		with codecs.open('./yahoo_test', 'wb', 'utf-8') as out:
			out.write(content)
		
		print '  Crawling Yahoo data...'
		self.yahoo_get_search_results(final_url, content)
		with codecs.open(os.path.join(self.output_root, 'yahoo.json'), 'wb', 'utf-8') as f:
			json.dump(self.results, f, indent=4)
		
	def yahoo_get_search_results(self, url, content):
		page = BeautifulSoup(content, 'lxml')

		if page.find('div', id='web'):
			list_results = page.find('div', id='web').find_all('li')
			# print len(list_results)
			for l in list_results:
				if l.find('div'):
					if l.find('div').find('h3'):
						if l.find('div').find('h3').find('a'):
							href_link = l.find('div').find('h3').a['href']
							if len(href_link) > 0:
								url = href_link
								if url not in self.url_list:
									self.url_list.append(url)
									if l.find('div').find('div', 'aAbs'):
										snippets = l.find('div').find('div', 'aAbs').get_text()
									else:
										snippets = u''

									self.results.append({'id':self.query_id+'_yahoo_'+str(self.crawl_idx), 'url':url, 'snippets':snippets})
					
									self.crawl_idx += 1
									#print self.pages_cnt,self.max_pages
									if self.crawl_idx > self.results_limit:
										self.crawl_idx = 1
										return
			
			if page.find('div', 'compPagination'):
				if page.find('div', 'compPagination').find('strong'):
					if page.find('div', 'compPagination').strong.find_next_sibling('a'):
						next_url = page.find('div', 'compPagination').strong.find_next_sibling('a')['href']	   
						next_url, next_content = self.get_page(next_url)
						#print next_url
						self.yahoo_get_search_results(next_url, next_content)
					else:
						return

	def start_crawl(self,source):
		print 'Query:' + self.query_id + " " +self.query
		if "all" in source:
			self.google_crawl()
			self.yahoo_crawl()
			self.bing_crawl_new()
		else:
			if "b" in source:
				self.bing_crawl_new()
			if "g" in source:
				self.google_crawl()
			if "y" in source:
				self.yahoo_crawl()

def load_queries(filename):
	queries_list = []
	with open(filename) as f:
		for line in f:
			line = line.strip()
			if line:
				row = line.split(':')
				queries_list.append((row[0],row[1]))

	return queries_list
	
	
if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--input", nargs='?', 
						help="Input file that contains queries. Format is QID:Query")
	parser.add_argument("-s", "--source", nargs='+', choices=['all','g','y','b'],
						help="Select the source for the snippets. Possilbe values are: all, g(oogle), y(ahoo), and b(ing)")
	args = parser.parse_args()
	filename = args.input
	source = args.source
	queries = load_queries(filename)

	for q in queries:
		snippets_crawler(q[0],q[1]).start_crawl(source)

