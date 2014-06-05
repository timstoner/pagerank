# reads a enwiki-(date)-page.sql file and outputs
# a file with page titles and wikipedia assigned page ids

import sys
import re
import datetime

pattern = re.compile("\(.*?,.*?,'.*?',.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?\)")
out_file = open("pages-simple.txt", 'w')

 

def main(path):
    print path
    
    starttime = datetime.datetime.now()
    print "Start at ", starttime
    # open page file
    page_file = open(path, 'r')
    
    linecount = 0
    msgcount = 0
    pagecount = 0
    log = "DTG: {} Line Count: {} Page Count: {}"
    
    # iterate over each line
    for line in page_file:
        # if the line starts with insert into page, handle line
        if str(line).startswith('INSERT INTO'):
            pagecount += handleInsertLine(line)
            linecount += 1
            msgcount += 1
            
        if msgcount == 100:
            print log.format(datetime.datetime.now(), linecount, pagecount)
            msgcount = 0

    print log.format(datetime.datetime.now(), linecount, pagecount)
    print "Number of Page: ", pagecount
    endtime = datetime.datetime.now()
    print "Start Time: ", starttime
    print "End Time: ", endtime
    print "Time Elapsed: ", endtime - starttime


def handleInsertLine(line):
    matches = re.findall(pattern, line)
    for match in matches:
        # print match
        #match_file.write(match)
        #match_file.write("\n")
        handleMatch(match)
        
    return len(matches)

def handleMatch(value):
    # print value[1:-1]
    # split on commas, removing first char '(' and last char ')'
    elements = str(value[1:-1]).split(',')
    # print elements
    # print elements
    pageid = elements[0]
    namespace = elements[1]
    if int(namespace) == 0:
        # second element is page title, remove surrounding ticks
        pagetitle = elements[2][1:-1]
        text = "{}!@#{}".format(pagetitle, pageid)
        out_file.write(text)
        out_file.write("\n")
    
if __name__ == '__main__':
    main(sys.argv[1])
