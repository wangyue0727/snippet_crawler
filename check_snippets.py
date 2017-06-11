import os
import json
import argparse

def main(all_topic):

	error={}
	error["google"]=[]
	error["yahoo"]=[]
	error["bing"]=[]

	all_queries={}

	path = "snippets/"
	
	with open(all_topic) as at:
		for line in at:
			qid,query = line.split(":")
			all_queries[qid]=query

	for name in os.listdir(path):
		path_name = os.path.join(path, name)
		# print path_name
		google_result = path_name+"/google.json"
		with open(google_result) as google:	
			data = json.load(google)
			length = 0
			if len(data) != 100:
				error["google"].append(name)
			
			# if len(data) == 0:
			# 	error["google"].append(name)
			# else:
			# 	for i in xrange(0,len(data)):
			# 		print i
			# 		length += len(data[i]["snippets"])
			# 	length/=len(data)
			# 	print length

		yahoo_result = path_name+"/yahoo.json"
		with open(yahoo_result) as yahoo:	
			data = json.load(yahoo)
			if len(data) != 100:
				error["yahoo"].append(name)

		bing_result = path_name+"/bing.json"
		with open(bing_result) as bing:	
			data = json.load(bing)
			if len(data) != 100:
				error["bing"].append(name)			
		error["google"].sort()
		error["yahoo"].sort()
		error["bing"].sort()

	try:
	    os.remove("google-recrawl.txt")
	    os.remove("yahoo-recrawl.txt")
	    os.remove("bing-recrawl.txt")
	except OSError:
	    pass


	if len(error["google"]) == 0 and len(error["yahoo"]) == 0 and len(error["bing"]) == 0:
		print "There is no error."
	else:
		if len(error["google"]) != 0:
			print "There are " + str(len(error["google"])) + " topics need to re-crawl on Google."
			with open("google-recrawl.txt","w") as f:
				for qid in error["google"]:
					f.write(qid+":"+all_queries[qid])
			print "Google re-do list has been saved to google-recrawl.txt"
			# print json.dumps(error["google"])

		if len(error["yahoo"]) != 0:
			print "There are " + str(len(error["yahoo"])) + " topics need to re-crawl on Yahoo."
			with open("yahoo-recrawl.txt","w") as f:
				for qid in error["yahoo"]:
					f.write(qid+":"+all_queries[qid])
			print "Yahoo re-do list has been saved to yahoo-recrawl.txt"

		if len(error["bing"]) != 0:
			print "There are " + str(len(error["bing"])) + " topics need to re-crawl on Bing."
			with open("bing-recrawl.txt","w") as f:
				for qid in error["bing"]:
					f.write(qid+":"+all_queries[qid])
			print "Bing re-do list has been saved to bing-recrawl.txt"

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--all", nargs='?', 
                        help="Input file that contains ALL queries. Format is QID:Query")
    args = parser.parse_args()
    filename = args.all
    main(filename)