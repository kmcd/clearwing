import urllib2
import sys

def usage():
    print "Usage: python download.py 'ticket name' into 'ticket file name'"

def getTicket( ticket ):
    ticketUrl = 'http://www.google.com/finance/getprices?q='\
                + ticket.upper() +\
                '&i=60&p=2d&f=d,o,h,l,c,v'
    request = urllib2.Request( ticketUrl )
    response = urllib2.urlopen( request )
    return response

def writeToFile( ticketFilename, data ):
    ticketFilename = ticketFilename
    ticketFile = open( ticketFilename, 'w' )
    try:
        for line in data:
            ticketFile.write( line )
    finally:
        ticketFile.close()
if __name__ == "__main__":
    ticket  = ''
    try:
        ticket = sys.argv[1]
    except:
        print 'First argument must be ticket name.'
        print usage()
        sys.exit(2)
    try:
        secondKeyword = sys.argv[2]
        if secondKeyword not in [ 'into' ]:
            raise Exception()
    except:
        print "Second argument must be keyword 'into'."
        print usage()
        sys.exit(2)
    ticketFilename = ''
    try:
        ticketFilename = sys.argv[3]
    except:
        print 'Third argument must be output file  name.'
        print usage()
        sys.exit(2)
    ticketData = None
    try:
        ticketData = getTicket( ticket )
    except:
        print "Unknown ticket #s" % ticket
    try:
        writeToFile( ticketFilename, ticketData )
    except:
        print "Error while writing into file %s:" % ticketFilename
