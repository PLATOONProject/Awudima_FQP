'''
Created on Jul 10, 2011

Implements the structures used by the ANAPSID Operators.

@author: Maribel Acosta Deibe
'''


class Record(object):
    '''
    Represents a structure that is inserted into the hash table.
    It is composed by a tuple, probeTS (timestamp when the tuple was probed)
    and insertTS (timestamp when the tuple was inserted in the table).
    '''

    def __init__(self, tuple, probeTS, insertTS=None, flushTS=None):
        self.tuple = tuple
        self.probeTS = probeTS
        self.insertTS = insertTS
        self.flushTS = flushTS


class RJTTail(object):
    '''
    Represents the tail of a RJT.
    It is composed by a list of records and rjtprobeTS
    (timestamp when the last tuple in the RJT was probed).
    '''

    def __init__(self, record, rjtProbeTS):
        self.records = [record]
        self.rjtProbeTS = rjtProbeTS
        self.flushTS = float("inf")

    def updateRecords(self, record):
        self.records.append(record)

    def setRJTProbeTS(self, rjtProbeTS):
        self.rjtProbeTS = rjtProbeTS


class FileDescriptor(object):
    '''
    Represents the description of a file, that contains a RJT in sec mem.
    It is composed by the name of the file, the associated resource,
    the current size (number of tuples), and the timestamp of the last
    RJTTail that have been flushed.
    '''

    def __init__(self, file, size, lastFlushTS):
        self.file = file
        self.size = size
        self.lastFlushTS = lastFlushTS
        # self.table = table

    def getSize(self):
        return self.size

#    def setSize(self, size):
#        self.size = size
#
#    def setLastFlushTS(self, lastFlushTS):
#        self.lastFlushTS = lastFlushTS


class NHJRecord(object):
    '''
    Represents a structure that is inserted into the hash table.
    It is composed by a tuple, ats (arrival timestamp) and
    dts (departure timestamp).
    '''

    def __init__(self, tuple, ats, dts):
        self.tuple = tuple
        self.ats = ats
        self.dts = dts

    def __repr__(self):
        return "(" + str(self.tuple) + ", " + str(self.ats) + ", " + str(self.dts) + ")"


class NHJPartition(object):
    '''
    Represents a bucket of the hash table.
    It is composed by a list of records, and a list of timestamps
    of the form {DTSlast, ProbeTS}
    '''

    def __init__(self):
        self.records = []  # List of records
        self.timestamps = []  # List of the form {DTSlas, ProbeTS}


class NHJTable(object):
    '''
    Represents a hash table.
    It is composed by a list of partitions (buckets) of size n,
    where n is specified in "size".
    '''

    def __init__(self):
        self.size = 1
        self.partitions = [NHJPartition() for x in range(self.size)]

    def getSize(self):
        return self.size

    def insertRecord(self, i, value):
        self.partitions[i].records.append(value)


class NHJFileDescriptor(object):
    '''
    Represents the description of a file, that contains a RJT in sec mem.
    It is composed by the name of the file, the associated resource,
    the current size (number of tuples), and the timestamp of the last
    RJTTail that have been flushed.
    '''

    def __init__(self, file, size, timestamps):
        # self.manager = Manager()
        self.file = file
        self.size = size
        self.timestamps = timestamps
        # self.timestamps = self.manager.list() #set()  #[]

    def getSize(self):
        return self.size


def isOverlapped(X, Y):
    (x1, x2), (y1, y2) = X, Y
    return (partiallyOverlapped((x1, x2), (y1, y2)) or
            fullyOverlapped((x1, x2), (y1, y2)))


def partiallyOverlapped(X, Y):
    (x1, x2), (y1, y2) = X, Y
    return ((x1 <= y1 <= x2 <= y2) or (y1 <= x1 <= y2 <= x2))


def fullyOverlapped(X, Y):
    (x1, x2), (y1, y2) = X, Y
    return ((x1 <= y1 <= y2 <= x2) or (y1 <= x1 <= x2 <= y2))

