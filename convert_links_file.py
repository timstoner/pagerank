import sys
import re
import datetime

# match tuple with integer id, 0 (namespace), and string
# starting with ' and ending with ' only after an es
pattern = re.compile(r"\([^0][0-9]*,0,'.*?[^\\']'\)")
out_file = open("links-simple.txt", 'w')
# match_file = open("links-match.txt", 'w')
# bad_file = open("bad-links.txt", 'w')

page_dict = {}

def main(path):
    print path
    
    readPageDict()
        
    starttime = datetime.datetime.now()
    print starttime
    # open page file
    link_file = open(path, 'r')
    
    msgcount = 0
    log = "DTG: {} Line Count: {} Link Count: {}"
    
    linkcount = 0
    linecount = 0

    currentpageid = 0
    currentpagevector = []
    
    # iterate over each line
    for line in link_file:
        # if line starts with insert, increment counter for logging
        if str(line).startswith('INSERT INTO '):
            linecount += 1
            msgcount += 1
        
        # find all link pairs in line
        matches = re.findall(pattern, line)
        for match in matches:
            linkcount += 1
            pageid, linkid = handleMatch(match)
            if linkid != -1:
                if pageid == currentpageid:
                    currentpagevector.append(linkid);
                else:
                    writeVector(currentpageid, currentpagevector)
                    currentpageid = pageid
                    currentpagevector = []
                    currentpagevector.append(linkid)
                
            
        if msgcount == 1000:
            print log.format(datetime.datetime.now(), linecount, linkcount)
            msgcount = 0

    print log.format(datetime.datetime.now(), linecount, linkcount)
    print "Number of Links: ", linkcount
    endtime = datetime.datetime.now()
    print "Start Time: ", starttime
    print "End Time: ", endtime
    print "Time Elapsed: ", endtime - starttime


def handleMatch(value):
    # print value[1:-1]
    # split on commas, removing first char '(' and last char ')'
    elements = str(value[1:-1]).split(',')
    # print elements
    pageid = elements[0]
    # second element is page title, remove surrounding ticks
    pagetitle = elements[2][1:-1]
    # check if title exists in map, otherwise discard it
    try:
        linkid = page_dict[pagetitle]
    except KeyError:
        linkid = -1
    
    return pageid, linkid
        
def writeVector(pageid, pagevector):
    header = "{}:".format(pageid)
    out_file.write(header)
    body = ""
    for i in pagevector:
        body += "{},".format(i)
    # chop off last comma
    out_file.write(body[:-1])
    out_file.write("\n")
    
        
def readPageDict():
    page_file = open("pages-simple.txt", 'r')
    print "start reading pages-simple.txt"
    for line in page_file:
        pair = line.split('!@#')
        title = pair[0]
        # get second element, strip off new line char
        pageid = pair[1][:-1]
        page_dict[title] = pageid
    print "end reading pages-simple.txt"
    print "length of page dict", len(page_dict)
    page_file.close()
    
    
if __name__ == '__main__':
    main(sys.argv[1])
