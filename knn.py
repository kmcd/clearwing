import kNN
import sys

def usage():
    print "Usage: python knn.py QQQ.csv 3 0.10"

def calculateLabel( dataAsMatrix ):
    DATE = 0
    CLOSE = 1
    HIGH = 2
    LOW = 3
    OPEN = 4
    VOLUME = 5
    dataLabels = []
    for index, item in enumerate( dataAsMatrix ):
        if item[ DATE ] == 391.:
            dataLabels.append( 'UNKNOWN' )
        else:
            nextItem = dataAsMatrix[ index ]
            firstCondition = ( nextItem[ LOW ] - item[ CLOSE ] ) > -0.03
            secondCondition = ( nextItem[ HIGH ] - item[ CLOSE ] ) >= 0.03
            thirdCondition = ( nextItem[ CLOSE ] - item[ CLOSE ] ) >= 0.03
            if firstCondition and ( secondCondition or thirdCondition ):
                dataLabels.append( 'LONG' )
            else:
                dataLabels.append( 'SHORT' )
    return dataLabels
if __name__ == "__main__":
    filename  = ''
    try:
        filename = sys.argv[1]
    except:
        print 'First argument must be ticket file name.'
        print usage()
        sys.exit(2)
    k  = ''
    try:
        k  = sys.argv[2]
    except:
        print 'Second argument must be k neighbor distance.'
        print usage()
        sys.exit(2)
    percentageOfTestDataSet = ''
    try:
        percentageOfTestDataSet  = sys.argv[3]
    except:
        print 'Third argument must be percentage of testing data.'
        print usage()
        sys.exit(2)
    dataStartsFromRow = 8
    dataStartsFromColumn = 1 
    numberOfFeatures = 5 
    calcClassLater = True
    separator = ','
    dataMatrix, dataLabels = kNN.file2matrix( filename, dataStartsFromRow, dataStartsFromColumn, numberOfFeatures, calcClassLater, separator )
    dataLabels = calculateLabel( dataMatrix )
    kNN.googleFinanceClassTest( float( percentageOfTestDataSet ), dataMatrix, dataLabels, int( k ) )
