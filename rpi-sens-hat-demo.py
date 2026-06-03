#!/usr/bin/env python3
# coding=utf-8

# Change to False when using the Sense HAT
# If vm == False, a startup of the emulator is required before running this demonstrator!
vm = True

import sys
import threading
import time
import logging
import operator
import matplotlib
matplotlib.use('Agg')  # Work around MacOS GUI backend in worker threads
import matplotlib.pyplot as plt
from datetime import datetime

if vm:
    from sense_emu import SenseHat
else:
    from sense_hat import SenseHat

DECAY_HUMIDITY = 1.00
DECAY_PRESSURE = 0.5 # bigger value if plotting
DECAY_COMPASS = 1.1
COLOR_BLUE = (0,0,255)

STAT_PLOT_STATISTICS = False
STAT_AMOUNT_OF_JOINS = 100
STAT_UPPER_BORDER = DECAY_PRESSURE * STAT_AMOUNT_OF_JOINS


class Stream:
    """
    Basisklasse für den Stream, der Funktionen wie Puffermanagement bereitstellt
    """
    def __init__(self, name, bufferSize, sid=None):
        """
        Erstellt einen neuen Datenstrom
        :param name: Name des Datenstromes für Debug-Ausgaben
        :param bufferSize: Größe des Puffers (Anzahl der speicherbaren Elemente)
        :param sid: Eindeutige Stream-ID für die Darstellung des Puffers auf dem LED-Board
        """
        self._name = name
        self._bufferSize = bufferSize
        self._buffer = [None] * bufferSize  # array with winsize None elements
        self._count = self._readPos = self._writePos = self._nextCandidateToJoinPointer = 0
        self._mutex = threading.Condition()
        self._sid = sid

    def __len__(self):
        """
        Gibt die Größe des Puffers zurück
        :return: Größe des Puffers
        """
        return len(self._buffer)  # or self._winsize

    def __str__(self):
        return "stream(name=%s, q=%s, cnt=%d, rpos=%d, wpos=%d, jp=%d)" % (
            self._name, str(self._buffer), self._count, self._readPos, self._writePos, self._nextCandidateToJoinPointer)

    def _isfull(self):
        """
        Gibt einen Boolean zurück, ob der Puffer vollständig gefüllt ist
        :return: Boolean, ob Puffer vollständig gefüllt ist
        """
        return self._readPos == self._writePos and self._count == self._bufferSize

    def _isempty(self):
        """
        Gibt einen Boolean zurück, ob der Puffer vollständig leer ist
        :return: Boolean, ob Puffer vollständig leer ist
        """
        return self._readPos == self._writePos and self._count == 0

    def getBufferSize(self):
        """
        Gibt die Puffergröße zurück
        :return: Integer mit Puffergröße
        """
        return self._bufferSize

    def _enqueue(self, t):
        """
        Speichert ein Tupel im Ringspeicher und zeigt ggf. die Belegung auf dem LED-Board an
        :param t: Zu speicherndes Tupel
        :return: void
        """
        self._count += 1
        self._buffer[self._writePos] = t
        if self._sid is not None and self._bufferSize <= 8:
            sensorSource.set_pixel(self._writePos, self._sid, COLOR_BLUE)
        self._writePos = (self._writePos + 1) % self._bufferSize

    def _dequeue(self):
        """
        Entfernt ein Tupel aus dem Puffer und deaktiviert ggf. seine Belegungsanzeige auf dem LED-Board
        :return: Zu lesendes Tupel
        """
        t = self._buffer[self._readPos]
        self._buffer[self._readPos] = None  # frees the object
        if self._sid is not None and self._bufferSize <= 8:
            sensorSource.set_pixel(self._readPos, self._sid, (0, 0, 0))
        self._readPos = (self._readPos + 1) % self._bufferSize
        self._count -= 1
        return t

    def _readWithoutDequeue(self):
        """
        Liest ein Tupel im Puffer an der Leseposition, ohne es zu entfernen
        :return: Tupel an der Stelle der Leseposition
        """
        return self._buffer[self._readPos]

    # public methods, the stuff above should only be called withing the guarded commands acquire()/release() to be thread-safe
    def put(self, t):
        """
        Kapselt die _enqueue Funktion mit Mutex zum Speichern eines Tupels im Ringspeicher
        :param t: Das zu speichernde Tupel
        :return: void
        """
        self._mutex.acquire()
        if self._isfull():
            logging.debug("WAITING Thread blocked by full buffer of output stream = %s" % str(self))
            self._mutex.wait()  # wait for tuples dequeued
            logging.debug("WORKING Buffer of output stream not full anymore")
        self._enqueue(t)
        self._mutex.notify()
        self._mutex.release()

    def get(self):
        """
        Kapselt die _dequeue Funktion mit Mutex zum Lesen eines Tupels im Ringspeicher
        :return: Tupel an der Leseposition
        """
        self._mutex.acquire()
        if self._isempty():
            logging.debug("WAITING Thread blocked by empty buffer of input stream")
            self._mutex.wait()  # wait for tuples enqueued
            logging.debug("WORKING Buffer of input stream not empty anymore. Buffer is now = %s" % str(self))
        t = self._dequeue()
        self._mutex.notify()
        self._mutex.release()
        return t


class StreamFMJSource(Stream):
    """
    Klasse für den Quelldatenstrom eines Fuzzy-Merge-Joins.
    """
    def getFMJCandidate(self, dominantTime):
        """
        Sucht den nächsten Verbundkandidaten für den Fuzzy-Merge-Join basierend auf dem Zeitstempel des dominanten Elementes
        :param dominantTime: Zeit des dominanten Elementes
        :return: Gefundenen Verbundkandidaten
        """
        candidateFound = False
        self._mutex.acquire()
        while not candidateFound:
            if(self._isempty()):
                logging.debug("WAITING Thread blocked by empty buffer of input stream")
                self._mutex.wait()  # wait for tuples dequeued
                logging.debug("WORKING Buffer of input stream not empty anymore. Buffer is now = %s" % str(self))
            t = self._dequeue()
            self._mutex.notify() # Notify threads which waited for a dequeue
            if t[0] < dominantTime: # t[0] represents always the time by assertion
                logging.debug("FMJ: %s is not a join partner for any dominant tuple, because its timestamp is not >= than %s, dropping this tuple", t, dominantTime)
            else:
                candidateFound = True
        self._mutex.release()
        return t


class StreamFMJBSource(Stream):
    """
    Klasse für den Quelldatenstrom eines bufferlosen Fuzzy-Merge-Joins
    """
    def getFMJBcandidate(self, dominantTime):
        """
        Sucht den nächsten Verbundkandidaten für den bufferlosen Fuzzy-Merge-Join basierend auf dem Zeitstempel des dominanten Elementes
        :param dominantTime: Zeit des dominanten Elementes
        :return: Gefundenen Verbundkandidaten
        """
        self._mutex.acquire()
        if (self._isempty()):
            logging.debug("WAITING Thread blocked by empty buffer of input stream")
            self._mutex.wait()  # wait for tuples dequeued
            logging.debug("WORKING Buffer of input stream not empty anymore. Buffer is now = %s" % str(self))
        t = self._readWithoutDequeue() # do not delete it from buffer yet
        if t[0] > dominantTime:
            logging.debug("Shouldn't happen at all")
            return None
        else :
            self._dequeue() # dequeue it now, it is a matching partner
        return t


class StreamMDJSource(Stream):
    """
    Klasse für den Quelldatenstrom eines Minimal-Delta-Join
    """
    def __init__(self, name, bufferSize, sid=None):
        """
        Initialisiert einen neuen Quelldatenstrom für einen Minimal Delta Join
        :param name: Name des Datenstromes für Debug-Ausgaben
        :param bufferSize: Größe des Puffers (Anzahl der speicherbaren Elemente)
        :param sid: Eindeutige Stream-ID für die Darstellung des Puffers auf dem LED-Board
        """
        parent = Stream.__init__(self, name, bufferSize, sid)
        self._windowStart = 0
        self._windowEnd = 0
        self.lastDominantElement = [-1,-1]
        self.statisticsOverwrittenElementCount = 0
        self.statisticsOverwrittenElements = []
        self.highestJoinedCandidate = [-1,-1]
        self.highestSelfJoinedElem = [-1, -1]

    def _enqueue(self, t):
        """
        Speichert ein Tupel im Ringspeicher und zeigt ggf. die Belegung auf dem LED-Board an
        Override der Basisklasse. Ermöglicht das Überschreiben alter Elemente
        :param t: Zu speicherndes Tupel
        :return: void
        """
        if not self._count == self._bufferSize: # don't exceed the count when overwriting tuples
            self._count += 1
        else: # full buffer, overwriting
            self.statisticsOverwrittenElementCount += 1
            self.statisticsOverwrittenElements.append([self.lastDominantElement[0], self.statisticsOverwrittenElementCount])
        self._buffer[self._writePos] = t
        self._windowEnd = (self._windowEnd + 1) % self._bufferSize
        if self._sid is not None and self._bufferSize <= 8:
            sensorSource.set_pixel(self._writePos, self._sid, COLOR_BLUE)
        self._writePos = (self._writePos + 1) % self._bufferSize

    def put(self, t):
        """
        Kapselt die _enqueue Funktion mit Mutex zum Speichern eines Tupels im Ringspeicher
        Override der Basisklasse. Ermöglicht das Überschreiben alter Elementes
        :param t: Das zu speichernde Tupel
        :return: void
        """
        self._mutex.acquire()
        if self._isfull():
            logging.warning("Buffer is full! Overwriting oldest tuple %s with new tuple %s! (wpos= %s, rpos= %s, joinpos= %s)", self._readWithoutDequeue(), t, self._writePos, self._readPos, self._nextCandidateToJoinPointer)
            if self._readPos == self._writePos:
                self._readPos = (self._readPos + 1) % self._bufferSize # adapt readPos to next oldest value in ring buffer
                logging.warning("  Also incrementing readPos")
            if self._nextCandidateToJoinPointer == self._writePos:
                self._nextCandidateToJoinPointer = (self._nextCandidateToJoinPointer + 1) % self._bufferSize
                logging.warning("  Also incrementing joinPointer")
            if self._windowStart == self._writePos:
                self._windowStart = (self._windowStart + 1) % self._bufferSize
                logging.warning("  Also incrementing Sliding Window start position")
            if self._windowEnd == self._writePos:
                self._windowEnd = (self._windowEnd - 1) % self._bufferSize
                logging.warning("  Also decrementing Sliding Window end position")

        self._enqueue(t)
        self._mutex.notify()
        self._mutex.release()

    def statistics_get_buffer_items(self):
        """
        Für Statistikmodul. Gibt Anzahl der Elemente im Puffer zurück
        :return: Anzahl der Elemente im Puffer
        """
        self._mutex.acquire()
        fullItems = 0
        for x in self._buffer:
            if x is not None:
                fullItems += 1
        self._mutex.release()
        return fullItems

    def getCandidateIndexesList(self, bufferLabel, printSW = False):
        """
        Generatorfunktion zum Generieren einer Liste von Werten (Pufferpositionen) mit Start und Endwert mit Modulo-Unterstützung
        :param bufferLabel: Für Debugausgabe. String, ob dominanter oder nicht-dominanter Stream
        :param printSW: Boolean zum Aktivieren der Debugausgabe
        :return:Liste von Pufferpositionen
        """
        result = []
        currIdx = self._windowStart
        while True:
            result.append(currIdx)
           # if currIdx == self._windowEnd:
            if currIdx == self._windowEnd:
                break
            currIdx = (currIdx + 1) % self.getBufferSize()
        if printSW == True:
            logging.info("Sliding Window of %s stream has a range of %s (windowStart=%s, windowEnd=%s)", bufferLabel, result, self._windowStart, self._windowEnd)
        return result

    def incrementNextCandidateToJoinPointer(self):
        """
        Inkrementiert den Pointer, der auf den nächsten Verbundausgangspunkt zeigt mit Modulo-Unterstützung
        :return: void
        """
        self._nextCandidateToJoinPointer = (self._nextCandidateToJoinPointer + 1) % self._bufferSize

    def readAtNextCandidateToJoinPointer(self):
        """
        Liest den nächsten Verbundkandidaten
        :return: Nächster Verbundkandidat
        """
        self._mutex.acquire()
        if self._isempty():
            logging.debug("WAITING Thread blocked by empty buffer of input stream")
            self._mutex.wait()  # wait for tuples dequeued
            logging.debug("WORKING Buffer of input stream not empty anymore. Buffer is now = %s" % str(self))
        if self._buffer[self._nextCandidateToJoinPointer] is None:
            logging.info("WAITING FOR NEW ITEM = %s" % str(self))
            self._mutex.wait()
        # candidate = self._readWithoutDequeue()
        candidate = self._buffer[self._nextCandidateToJoinPointer]
        # self._mutex.notify()
        self._mutex.release()
        return candidate

    def getMDJMinimalDelta(self, dominantTime):
        """
        Berechnet das minimale Delta zu den nicht-dominanten Tupeln in Abhängigkeit von dem dominanten Zeitstempel
        :param dominantTime: Zeitstempel des dominanten Tupels
        :return: Minimales Delta
        """
        logging.info("Looking for min-delta according to tuple with timestamp %f", float(dominantTime))
        minimalDelta = float("inf")
        minimalDeltaCandidates = []
        possibleMDCandidates = self.getCandidateIndexesList("REC", True)
        while len(minimalDeltaCandidates) == 0: # while no candidates found
            self._mutex.acquire()
            #logging.debug("BUFFER of REC S is %s", self._buffer)
            if (self._isempty()):
                logging.debug("WAITING Thread blocked by empty buffer of input stream")
                self._mutex.wait()  # wait for tuples dequeued
                logging.debug("WORKING Buffer of input stream not empty anymore. Buffer is now = %s" % str(self))

            # Wait until there is at least one element in buffer that is bigger or equal than the current element
            oneBiggerElementFound = False
            while oneBiggerElementFound == False: # and self._isfull() == False:
                for item in [self._buffer[i] for i in self.getCandidateIndexesList("REC", False)]: # search for bigger candidates inside the window
                    if item is not None:
                        if item[0] >= dominantTime:
                            logging.debug("Found at least one element with bigger Timestamp: %s", item)
                            try:
                                self._windowEnd = self._buffer.index(item)
                            except ValueError:
                                logging.error("This should never happen. The found item is not in the list anymore")
                            oneBiggerElementFound = True
                        else:
                            self._windowEnd = (self._windowEnd +1) % self._bufferSize # otherwise we will never find sth in the window

            # Search minimal delta in buffer
            for item in [self._buffer[i] for i in possibleMDCandidates]: # only determine min-delta inside the window
                if item is not None:
                    minimalDelta = abs(dominantTime - item[0]) if abs(dominantTime - item[0]) < minimalDelta else minimalDelta

            self._mutex.notify()
            self._mutex.release()
            logging.info("Minimal delta found: %s", minimalDelta)
            return minimalDelta

    def getMDJJoinCandidates(self, dominantTime, minimalDelta):
        """
        Sucht alle Kandidaten für ein Tupel mit gegebenen minimalem Delta
        :param dominantTime: Zeitstempel des dominanten Tupels
        :param minimalDelta: Minimales Delta
        :return: Liste aller Verbundkandidaten
        """
        logging.debug("Looking for MDJ candidates with minimal delta of %s", minimalDelta)
        minimalDeltaCandidates = []
        while len(minimalDeltaCandidates) == 0:
            self._mutex.acquire()
            #logging.debug("BUFFER of REC S is %s", self._buffer)
            if (self._isempty()):
                logging.info("WAITING Thread blocked by empty buffer of input stream")
                self._mutex.wait()  # wait for tuples dequeued
                logging.debug("WORKING Buffer of input stream not empty anymore. Buffer is now = %s" % str(self))

            # Add items with minimal delta to list
            elementsInWindow = self.getCandidateIndexesList("REC")
            for item in [self._buffer[i] for i in elementsInWindow]: # Only look inside window
                if item is not None:
                    if abs(dominantTime - item[0]) == minimalDelta:
                        minimalDeltaCandidates.append(item)

                        if item[0] >= dominantTime:
                            # we have a min delta with a bigger value
                            # track this to avoid a double join after a dominance swap
                            # currently we are in the recessive stream (important for later!)
                            self.highestJoinedCandidate = item

                    #if item[0] - minimalDelta > dominantTime: # emulate end of sliding window, do not check candidates out of window
                    #    break;
            self._mutex.notify()
            self._mutex.release()
        logging.debug("Minimal Delta Candidate Set found: %s, candidates were tuples with indexes %s", minimalDeltaCandidates, elementsInWindow)
        return minimalDeltaCandidates

    def removeOldMDJItems(self, threshold, bufferLabel):
        """
        Löscht alle Tupel aus einem Ringspeicher, die nicht mehr Verbundkandidat im Minimal-Delta-Join werden können
        :param threshold: Zeitliche Grenze, unter der gelöscht wird
        :param bufferLabel: Für Debugausgabe. String ob dominanter oder nicht-dominanter Stream
        :return:
        """
        logging.info("Removing all tuples with a smaller timestamp than %f from buffer of %s stream", float(threshold), bufferLabel)
        # pop items until threshold is reached. Works because items are assumed to be in order
        self._mutex.acquire()
        while True:
            t = self._readWithoutDequeue() # Possible to do this since elements are implicit ordered by time
            logging.debug("Reading %s", t)
            if t is not None:
                if t[0] < threshold:
                    self.get()
                    logging.info("  Removed %s from %s buffer (threshold was %f)", t, bufferLabel, float(threshold))
                    # lowering end of sliding window
                    self._windowStart = (self._windowStart + 1) % self._bufferSize
                    logging.info("    Adapted lower end of sliding window of %s stream (windowEnd=%s)", bufferLabel, self._windowStart)
                else:
                   # logging.info("  No tuples removed")
                    break
        logging.info("  BUFFER REC: %s", str(self))
        self._mutex.notify()
        self._mutex.release()

'''
Input Sources
'''

humidityFallbackWarned = False
pressureFallbackWarned = False
compassFallbackWarned = False

def safe_get_humidity():
    global humidityFallbackWarned
    try:
        return sensorSource.get_humidity()
    except OSError as e:
        if not humidityFallbackWarned:
            logging.warning("Humidity sensor failed: %s. Using fallback value 50.0.", e)
            humidityFallbackWarned = True
        return 50.0


def safe_get_pressure():
    global pressureFallbackWarned
    try:
        return sensorSource.get_pressure()
    except OSError as e:
        if not pressureFallbackWarned:
            logging.warning("Pressure sensor failed: %s. Using fallback value 1013.25.", e)
            pressureFallbackWarned = True
        return 1013.25


def safe_get_compass():
    global compassFallbackWarned
    try:
        return sensorSource.get_compass()
    except OSError as e:
        if not compassFallbackWarned:
            logging.warning("Compass sensor failed: %s. Using fallback value 0.0.", e)
            compassFallbackWarned = True
        return 0.0


def fetchHumidityData(oStream):
    """
    Liest kontinuierlich Luftfeuchtigkeitsdaten aus, verknüft die Daten mit einem Timestamp und speichert die Daten in einem Stream
    :param oStream: Stream, in welchem die Luftfeuchtigkeitsdaten gespeichert werden sollen
    :return: void
    """
    while True:
        humidity = safe_get_humidity()
        humidity = [time.time(), float(humidity)]
        logging.info("[==>] Humidity[%f, %f]", float(humidity[0]), float(humidity[1]))
        oStream.put(humidity)
        time.sleep(DECAY_HUMIDITY)


def fetchHumidityDataLC(oStream):
    """
    Liest kontinuierlich Luftfeuchtigkeitsdaten aus, verknüft die Daten mit einem Timestamp einer logischen Uhr und speichert die Daten in einem Stream
    :param oStream: Stream, in welchem die Luftfeuchtigkeitsdaten gespeichert werden sollen
    :return: void
    """
    clock = float(0.0)
    while True:
        humidity = safe_get_humidity()
        humidity = [clock, float(humidity)]
        clock = clock + float(DECAY_HUMIDITY)
        logging.info("[==>] Humidity[%f, %f]", float(humidity[0]), float(humidity[1]))
        oStream.put(humidity)
        time.sleep(DECAY_HUMIDITY * 0.995)


def fetchPressureData(oStream):
    """
    Liest kontinuierlich Luftdruckdaten aus, verknüft die Daten mit einem Timestamp und speichert die Daten in einem Stream
    :param oStream: Stream, in welchem die Luftdruckdaten gespeichert werden sollen
    :return: void
    """
    while True:
        pressure = safe_get_pressure()
        pressure = [time.time(), float(pressure)]
        logging.info("[==>] Pressure[%f, %f]", float(pressure[0]), float(pressure[1]))
        oStream.put(pressure)
        time.sleep(DECAY_PRESSURE)


def fetchPressureDataLC(oStream):
    """
    Liest kontinuierlich Luftdruckdaten aus, verknüft die Daten mit einem Timestamp einer logischen Uhr und speichert die Daten in einem Stream
    :param oStream: Stream, in welchem die Luftdruckdaten gespeichert werden sollen
    :return: void
    """
    clock = float(0.0)
    while True:
        pressure = safe_get_pressure()
        pressure = [clock, float(pressure)]
        clock = clock + float(DECAY_PRESSURE)
        logging.info("[==>] Pressure[%f, %f]", float(pressure[0]), float(pressure[1]))
        oStream.put(pressure)
        time.sleep(DECAY_PRESSURE)


def fetchCompass(oStream):
    """
    Liest kontinuierlich Kompassdaten (Grad) aus, verknüft die Daten mit einem Timestamp und speichert die Daten in einem Stream
    :param oStream: Stream, in welchem die Kompassdaten gespeichert werden sollen
    :return: void
    """
    while True:
        compass = safe_get_compass()
        compass = [time.time(), float(compass)]
        logging.debug("Compass is %f at time %s", compass[1], compass[0])
        oStream.put(compass)
        time.sleep(DECAY_COMPASS)


'''
Operations
'''


def selectionOperation(iStream, oStream, selectionPredicateList):
    """
    Implementiert die Selektionsoperation
    :param iStream: Eingangsdatenstrom, auf dem die Selektionsoperation angewendet werden soll
    :param oStream: Ausgangsdatenstrom, in dem die Elemente, die das Selektionsprädikat erfüllen, gespeichert werden sollen
    :param selectionPredicateList: Liste der Selektionsprädikate
    :return: void
    """
    while True:
        selectionPredicateSatisfied = True
        ops = {'>': operator.gt, '<': operator.lt, '>=': operator.ge, '<=': operator.le, '=': operator.eq,
               '!=': operator.ne}
        iStreamItem = iStream.get()

        for selectionCondition in selectionPredicateList:
            if ops[selectionCondition[1]](iStreamItem[selectionCondition[0]], selectionCondition[2]):
                pass
            else:
                selectionPredicateSatisfied = False
                logging.fatal("Ignored %s (condition %s is False)", iStreamItem, str(selectionCondition))
                break

        if selectionPredicateSatisfied:
            logging.fatal("Added %s to OutputStream (conditions %s is/are true)", iStreamItem, str(selectionPredicateSatisfied))
            oStream.put(iStreamItem)


def projectionOperation(iStream, oStream, projectionList):
    """
    Implementiert die Projektionsoperation
    :param iStream: Eingangsdatenstrom, auf dem die Projektionsoperation angewendet werden soll
    :param oStream: Ausgangsdatenstrom, in dem die Elemente gespeichert werden sollen, nachdem die Projektion angewendet wurde
    :param projectionList: Liste der Projektionsattribute
    :return:
    """
    if 0 not in projectionList:
        logging.warning("Timestamp not in projection list! Automatically added index 0 to projection list!")
        projectionList = (0,) + projectionList
    while True:
        iStreamItem = iStream.get()
        oStreamItem = list(iStreamItem[i] for i in projectionList)
        logging.debug("%s is now %s (Projection on indexes %s)", iStreamItem, oStreamItem, projectionList)
        oStream.put(oStreamItem)


def streamExtremaOperation(iStream, oStream, extremaPosition, extremaOperation):
    """
    Extremwertbestimmungsoperation innerhalb eines Streams
    :param iStream: Eingangsdatenstrom, in dem nach einem Extremum gesucht werden soll
    :param oStream: Ausgangsdatenstrom, in den ein Tupel mit gefundenem Extremum geschrieben werden soll
    :param extremaPosition: Index, an welcher Stelle im Tupel nach dem Extremung gesucht werden soll
    :param extremaOperation: Angabe, ob ein Minimum oder Maximum gesucht werden soll. Für ein Maximum muss als Parameter ein ">" übergeben werden, für ein Minimum "<". Standardmäßig wird das Minimum gesucht
    :return: void
    """
    ops = {'>': operator.gt, '<': operator.lt}
    currentExtrema = float("-inf") if extremaOperation == ">" else float("inf") # Init with Anti-Extrema
    extremaOperationLabel = "maxima" if extremaOperation == ">" else "minima"
    while True:
        iStreamItem = iStream.get()
        if ops[extremaOperation](iStreamItem[extremaPosition], currentExtrema):
            currentExtrema = iStreamItem[extremaPosition]
            oStream.put(iStreamItem)
            logging.debug("Found new %s: %s in tuple %s", extremaOperationLabel, iStreamItem[extremaPosition], iStreamItem)


def windowExtremaOperation(iStream, oStream, extremaPosition, extremaOperation):
    """
    Extremwertbestimmungsoperation innerhalb eines Fensters (Puffers)
    :param iStream: Eingangsdatenstrom, in dem nach einem Extremum gesucht werden soll
    :param oStream: Ausgangsdatenstrom, in den ein Tupel mit gefundenem Extremum geschrieben werden soll
    :param extremaPosition: Index, an welcher Stelle im Tupel nach dem Extremung gesucht werden soll
    :param extremaOperation: Angabe, ob ein Minimum oder Maximum gesucht werden soll. Für ein Maximum muss als Parameter ein ">" übergeben werden, für ein Minimum "<". Standardmäßig wird das Minimum gesucht
    :return: void
    """
    ops = {'>': operator.gt, '<': operator.lt}
    extremaOperationLabel = "maxima" if extremaOperation == ">" else "minima"
    while True:
        currentExtrema = float("-inf") if extremaOperation == ">" else float("inf")  # Init with Anti-Extrema
        buffCache = list(iStream._buffer) # list to avoid call by reference
        extremaItem = None
        logging.critical(str(buffCache))
        for bufferItem in buffCache:
            if bufferItem is not None:
                if ops[extremaOperation](bufferItem[extremaPosition], currentExtrema):
                    currentExtrema = bufferItem[extremaPosition] # new extrema founds
                    extremaItem = bufferItem
        if not (currentExtrema == float("-inf") or currentExtrema == float("inf")): # Buffer consists of None
            oStream.put(extremaItem)
            logging.critical("Found %s: %s", extremaOperationLabel, currentExtrema)
        while buffCache == iStream._buffer: # Wait until buffer has new item
            pass
        if iStream._isfull():
            iStream.get() # start deleting old items


def fuzzyMergeJoin(iStreamDominant, iStreamRecessive, oStream):
    """
    Implementierung des Fuzzy-Merge-Joins
    :param iStreamDominant: Dominanter Datenstrom der Klasse StreamFMJSource
    :param iStreamRecessive: Nicht-dominanter DAtenstrom der Klasse StreamFMJSource
    :param oStream: Datenstrom, in den verbundene Elemente eingefügt werden sollen
    :return: void
    """
    while True:
        iStreamDominantItem = iStreamDominant.get()
        iStreamRecessiveItem = iStreamRecessive.getFMJCandidate(iStreamDominantItem[0])
        oStreamItem = iStreamDominantItem + iStreamRecessiveItem[1:] # ignore the time of iStreamRecessiveItem
        logging.debug("Joined tuple found: %s, consists of %s (dom) and %s (rec)", oStreamItem, iStreamDominantItem, iStreamRecessiveItem)
        oStream.put(oStreamItem)


def fuzzyMergeJoinBufferless(iStreamDominant, iStreamRecessive, oStream):
    """
    Implementierung des bufferlosen Fuzzy-Merge-Joins
    :param iStreamDominant: Dominanter Datenstrom der Klasse StreamFMJBSource
    :param iStreamRecessive: Nicht-dominanter DAtenstrom der Klasse StreamFMJBSource
    :param oStream: Datenstrom, in den verbundene Elemente eingefügt werden sollen
    :return: void
    """
    while True:
        iStreamDominantItem = iStreamDominant.get()
        iStreamRecessiveItem = iStreamRecessive.getFMJBcandidate(iStreamDominantItem[0])
        if iStreamRecessiveItem is None: # no matching partner
            logging.debug("No join partner for dominant tuple %s found, dropping this tuple", iStreamDominantItem)
        else: # matching partner found
            oStreamItem = iStreamDominantItem + iStreamRecessiveItem[1:] # ignore the time of iStreamRecessiveItem
            logging.debug("Joined tuple found: %s, consists of %s (dom) and %s (rec)", oStreamItem, iStreamDominantItem, iStreamRecessiveItem)
            oStream.put(oStreamItem)


def minimalDeltaJoin(iStreamOne, iStreamTwo, oStream):
    """
    Implementierung des Minimal-Delta-Joins
    :param iStreamOne: Ein Datenstrom der Klasse StreamMDJSource
    :param iStreamTwo: Ein Datenstrom der Klasse StreamMDJSource
    :param oStream: Datenstrom, in den verbundene Elemente eingefügt werden sollen
    :return: void
    """

    mdjStart = datetime.now()
    listOfTimes = []
    listOfDominantBufferItems = []
    listOfRecessiveBufferItems = []
    numpyCnt = 0
    fig, ax = plt.subplots()
    axes = [ax, ax.twinx(), ax.twinx(), ax.twinx()]
    fig.subplots_adjust(right=0.75)
    axes[1].spines['right'].set_position(('axes', 1.15))
    axes[2].spines['right'].set_position(('axes', 1.25))
    axes[1].set_frame_on(True)
    axes[1].patch.set_visible(False)
    # Arbitrary initialization
    dominantStream = iStreamOne
    recessiveStream = iStreamTwo
    while True:
        logging.info("")
        logging.info("+++++ Beging New Minimal Delta Join Phase +++")
        #time.sleep(0.1)


        while (dominantStream.readAtNextCandidateToJoinPointer())[0] <= dominantStream.highestSelfJoinedElem[0]:
            # MDJ ist faster than tuple generating
            # Full round in ring buffer reached
            # Slow it down a bit to avoid re-join of ring buffer items
            pass

        currentDominantCandidate = dominantStream.readAtNextCandidateToJoinPointer()
        currentRecessiveCandidate = recessiveStream.readAtNextCandidateToJoinPointer()
        logging.info("BUFFER DOM: %s", str(dominantStream))
        logging.info("BUFFER REC: %s", str(recessiveStream))

        # Compare candidates to check next dominant item
        if(currentRecessiveCandidate[0] < currentDominantCandidate[0]): # swap dominance if current recessive stream has smaller timestamp value at pointer
            logging.info("[<->] MDJ Dominance swap! %f is now smaller than %f", float(currentRecessiveCandidate[0]), float(currentDominantCandidate[0]))
            dominantStream._mutex.acquire()
            recessiveStream._mutex.acquire()
            tmp = dominantStream
            dominantStream = recessiveStream
            recessiveStream = tmp
            dominantStream._mutex.release()
            recessiveStream._mutex.release()
            del tmp

            # now we have to have a look in the dominant stream
            if dominantStream.readAtNextCandidateToJoinPointer() == dominantStream.highestJoinedCandidate:
                logging.critical("Already joined candidate over here!. %s of the DOM stream was already a join partner when it was in the REC stream. Adapting logic and joining next element", str(dominantStream.highestJoinedCandidate))
                dominantStream.incrementNextCandidateToJoinPointer()
                recessiveStream.removeOldMDJItems(dominantStream.highestJoinedCandidate[0], "REC")
                continue # restart the MDJ process

            logging.info("  BUFFER DOM: %s", str(dominantStream))
            logging.info("  BUFFER REC: %s", str(recessiveStream))

        dominantCandidate = dominantStream.readAtNextCandidateToJoinPointer()
        dominantStream.lastDominantElement = dominantCandidate

        # Check for: MDJ ist faster than tuple generating
        # Slow it down a bit to avoid re-join of ring buffer items
        if dominantStream.highestSelfJoinedElem[0] < dominantCandidate[0]:
            dominantStream.highestSelfJoinedElem = dominantCandidate

        logging.info("Looking for a partner for %s", dominantCandidate)
        minimalDelta = recessiveStream.getMDJMinimalDelta(dominantCandidate[0])
        recessiveCandidates = recessiveStream.getMDJJoinCandidates(dominantCandidate[0], minimalDelta)
        for candidate in recessiveCandidates:
            oStreamItem = dominantCandidate + candidate[1:]
            logging.fatal("New MDJ tuple found: %s, derived from tuples %s and %s (min-delta was %s)", oStreamItem, dominantCandidate, candidate, abs(oStreamItem[0] - candidate[0]))
            oStream.put(oStreamItem)

            # Track and plot statistics stuff
            listOfTimes.append([dominantCandidate[0], candidate[0]])
            listOfDominantBufferItems.append([dominantCandidate[0], dominantStream.statistics_get_buffer_items()])
            listOfRecessiveBufferItems.append([dominantCandidate[0], recessiveStream.statistics_get_buffer_items()])
            numpyCnt = numpyCnt + 1
            if STAT_PLOT_STATISTICS and numpyCnt == STAT_AMOUNT_OF_JOINS:
                logging.info("[/\\/] Drawing statistics...")
                mdjEnd = datetime.now()

                skippedElements = dominantStream.statisticsOverwrittenElements
                logging.info("List of skipped elements: " + str(skippedElements))

                for ax in axes:
                    ax.set_xlim(-0.01, listOfTimes[numpyCnt-1][0] + 0.1)
                    ax.set_ylim(-0.01, listOfTimes[numpyCnt-1][1] + 0.1)
                axes[0].set_xlabel("Dominanter Zeitstempel", color='Blue')
                axes[0].set_ylabel('Nichtdominanter Zeitstempel', color='Blue')
                axes[1].set_ylabel('Pufferbelegung Dominant [Anz. Tupel]', color='Red')
                axes[2].set_ylabel('Pufferbelegung Nicht-Dominant [Anz. Tupel]', color='Green')
                axes[3].set_ylabel('$\sum$ Ueberschriebene Pufferelemente [Anz. Tupel]', color='Orange')
                #axes[1].set_autoscale_on(False)
                #axes[2].set_autoscale_on(False)
                axes[1].set_ylim([0, dominantStream._bufferSize])
                axes[2].set_ylim([0, recessiveStream._bufferSize])
                axes[0].scatter(*zip(*listOfTimes), marker='x')
                axes[1].scatter(*zip(*listOfDominantBufferItems), color='Red', marker='^')
                axes[2].scatter(*zip(*listOfRecessiveBufferItems), color='Green', marker='v')
                if len(skippedElements) > 0:
                    axes[3].set_ylim([0, len(skippedElements)])
                    axes[3].scatter(*zip(*skippedElements), color='Orange', marker='.')
                else:
                    axes[3].set_ylim([0, 1])
                # axes[3].plot(*zip(*skippedElements), color='Orange')

                axes[0].plot([0, STAT_UPPER_BORDER], [0, STAT_UPPER_BORDER])

                runtime = mdjEnd - mdjStart
                plt.title('Messfrequenzen ' + str(DECAY_HUMIDITY) + ' sec und ' + str(DECAY_PRESSURE) + ' sec\n' + str(numpyCnt) + ' Joins, Laufzeit der Joins: ' + str(runtime.seconds) + ',' + str(runtime.microseconds) + ' sec', fontsize=14)
                fig.savefig('mdj_stats.png')
                logging.info("Saved MDJ statistics plot to mdj_stats.png")
                sys.exit() # doesnt work

        '''
        remove old tuples from recessive stream
        it is not possible to do this for the dominant stream because we do not know the min-delta for the recessive stream
        because the dominant and the recessive stream toggles once in a while we can delete them later
        since there cannot be a join partner with a timestamp smaller than the current recessive candidate, it is possible to delete all smaller elements
        '''

        timeOfRecessiveCandidate = recessiveCandidates[0]
        recessiveStream.removeOldMDJItems(timeOfRecessiveCandidate[0], "recessive")
        # dominantStream.get() # wrong!!! cannot be removed !!!
        dominantStream.incrementNextCandidateToJoinPointer()

        logging.info("+++++ End Of Minimal Delta Join Phase +++")
        logging.info("")


def sink(iStream):
    """
    Definiert eine Datensenke, die Daten konsumiert und anzeigt
    :param iStream: der Eingangsdatenstrom, dessen Elemente angezeigt werden sollen
    :return: void
    """
    allMatches = []
    while True:
        num = iStream.get()
        allMatches.append(num)
        logging.info("Consumed %s", num)


"""
Basic demonstrator setup
"""


sensorSource = SenseHat()
sensorSource.clear()
logging.basicConfig(level=logging.INFO, format='[%(levelname)-7s] (%(threadName)-10s) %(message)s')

if vm:
    logging.info("Using emulator package sense_emu")
else:
    logging.info("Using live data from sense_hat")

if DECAY_HUMIDITY > 0:
    logging.debug("Set humidity decay parameter to %s", DECAY_HUMIDITY)
else:
    logging.debug("No humidity decay, live data")


sensorSource.show_message("Don Quichote!", text_colour=[255, 0, 0])

#sensorSource.load_image("/usr/share/icons/Adwaita/cursors/icon")


"""
Minimal Working Example
"""


humidityStream = StreamMDJSource("Humidity", 8, sid=0)
selectionStream = StreamMDJSource("Selection", 8, sid=2)

pressureStream = StreamMDJSource("Pressure", 8, sid=1)
#fuzzyMergeJoinStream = Stream("FMJoin", 4)
#fuzzyMergeJoinBufferlessStream = Stream("FMJoin/B", 1)
minimalDeltaJoinStream = Stream("MDJoin", 8, sid=3)
#projectionStream = Stream("Projection 0 Stream", 4)
#extremaStream = Stream("Maxima Stream", 4)
sinkStream = Stream("Print Stream", 8, sid=4)


# Defining Threads for Streams

humidityThread = threading.Thread(name='humidity', target=fetchHumidityDataLC, args=(humidityStream,))
pressureThread = threading.Thread(name='pressure', target=fetchPressureDataLC, args=(pressureStream,))

# Filter Humidity, use only elements with a timestamp > 1 and a humidity >= 40%
selPreds = [[0, ">", 1], [1, ">=", 20]]
selectionThread = threading.Thread(name='selection', target=selectionOperation, args=(humidityStream, selectionStream, selPreds))

minimalDeltaJoinThread = threading.Thread(name='md join', target=minimalDeltaJoin, args=(selectionStream, pressureStream, minimalDeltaJoinStream))

sinkThread = threading.Thread(name='sink', target=sink, args=(minimalDeltaJoinStream,))


# Start all threads

sinkThread.start()
minimalDeltaJoinThread.start()
selectionThread.start()
pressureThread.start()
humidityThread.start()


#fuzzyMergeJoinThread = threading.Thread(name='fm join', target=fuzzyMergeJoin, args=(humidityStream, pressureStream, fuzzyMergeJoinStream))
#fuzzyMergeJoinBufferlessThread = threading.Thread(name='fm b join', target=fuzzyMergeJoinBufferless, args=(pressureStream, humidityStream, fuzzyMergeJoinBufferlessStream))
#projectionThread = threading.Thread(name='projection', target=projectionOperation, args=(selectionStream, projectionStream, (0,1)))
#maximaThread = threading.Thread(name='maxima', target=globalExtremaOperation, args=(projectionStream, extremaStream, 1, ">"))

# Reverse Order Thread Startup

#sinkThread.start()
#maximaThread.start()
#projectionThread.start()
#selectionThread.start()

#fuzzyMergeJoinBufferlessThread.start()
