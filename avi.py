#!/usr/bin/python

import sys
import os
from re import search
from subprocess import PIPE, Popen

#configure the three parameters below
#1. The name of all the hosts in the cluster that will participate
hostList = ['insdanielblanxart','compute-0-0','compute-0-1','compute-0-2','compute-0-3']
#2.Float number obtained from benchmark of video conversing, the benchmark of the computer node have to be in the same position of the array.
benchList = [0.13679245283,0.5,0.160377358491,0.103773584906,0.0990566037736]
#3. The NFS mounted dir which contains the video you need encoded
encodeDir = "/nfs"

#Video diferent parameters
container = "avi"

#Function definitions

def getTotalJob(totalFrames, fps):
	return totalFrames / float(fps)

##Specific cluster functions
def getJobsDuration(totalJob):
	jobsDuration=[]
	totalFloat=0
	totalInt=0
	j=0
	#Calculate the duration job in a float number
	for i in benchList:
		jobsDuration.append( totalJob * i)
		j+=1
	j=0
	#Transforms the jobs to a number with two decimals
	for i in jobsDuration:
		divNumber=100
		job = float(i)
		job *= 100
		numbers = str(job).split('.')
		for i in range(len(numbers[1])):
			divNumber*=10
		totalFloat1 = int(numbers[1]) + totalInt
		totalFloat = float(totalInt)/ divNumber
		job /= 100
		job -= (float(numbers[1]) / divNumber)
		jobsDuration[j]=job
		j+=1
	jobsDuration[0]+=totalFloat
	return jobsDuration
        

def getFps(file):
	information = Popen(("ffmpeg", "-i", file), stdout=PIPE, stderr=PIPE)
	#fetching tbr (1), but can also get tbn (2) or tbc (3)
	#examples of fps syntax encountered is 30, 30.00, 30k
	fpsSearch = search("(\d+\.?\w*) tbr, (\d+\.?\w*) tbn, (\d+\.?\w*) tbc", information.communicate()[1])
	return fpsSearch.group(1)

def getTotalFrames(file, fps):
	information = Popen(("ffmpeg", "-i", file), stdout=PIPE, stderr=PIPE)
	timecode = search("(\d+):(\d+):(\d+).(\d+)", information.communicate()[1])
	return ((((float(timecode.group(1)) * 60) + float(timecode.group(2))) * 60) + float(timecode.group(3)) + float(timecode.group(4))/100) * float(fps)

def clusterRun(file, fileName, jobsDuration, fps):
	start = 0.0
	runCount=0
	jobList=[]
	#submits equal conversion portions to each host
	for i in hostList:
		end = jobsDuration[runCount]
		runCount += 1
		runFfmpeg = "ssh %s \'cd %s;ffmpeg -ss %f -t %f -y -i %s -b 512k -ar 44100 -ac 2 -ab 128k %s.%s &&>/dev/null\'" % (i, encodeDir, start, end, file, fileName + "_run" + str(runCount) , container)
		print("runffmpeg")
		print(runFfmpeg)
		start += end + 1/float(fps)
		jobList.append(Popen(runFfmpeg, shell=True))
	#wait for all jobs to complete
	runCount=0
	for i in hostList:
		jobList[runCount].wait()
		runCount += 1
	#append/rebuild final from parts and rebuild index
	mencoderHead = "mencoder -oac copy -ovc copy -o /var/www/html/cluster/videos/%sFinal.%s" % (fileName, container)
	mencoderTail = " %s_run1.%s %s_run2.%s" % (fileName,container,fileName,container)
	#add --appends for additional host above the first 2
	for i in range(len(hostList)- 2):
		mencoderTail = "%s %s_run%d.%s " % (mencoderTail, fileName, i+3, container)
	runAvidemux = "%s %s" % (mencoderHead, mencoderTail)
	var=Popen(runAvidemux, shell=True)	
	



#Main begin
sourceFile = sys.argv[1]
fps = getFps(sourceFile)
totalFrames = getTotalFrames(sourceFile, fps)
totalJob = getTotalJob(totalFrames, fps)
jobsDuration=getJobsDuration(totalJob)
fileName = os.path.splitext(sourceFile)[0]

clusterRun(sourceFile, fileName, jobsDuration, fps)

