"""
Python wrapper to download data through the Bloomberg Open API
Written by Alexandre Almosni   alexandre.almosni@gmail.com
(C) 2014-2017 Alexandre Almosni
Released under Apache 2.0 license. More info at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import print_function
from abc import ABCMeta, abstractmethod
import blpapi
import datetime
import pandas
import threading

#This makes successive requests faster
DATE             = blpapi.Name("date")
ERROR_INFO       = blpapi.Name("errorInfo")
EVENT_TIME       = blpapi.Name("EVENT_TIME")
FIELD_DATA       = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID         = blpapi.Name("fieldId")
SECURITY         = blpapi.Name("security")
SECURITY_DATA    = blpapi.Name("securityData")

################################################
class BLP2():
    """Naive implementation of the Request/Response Paradigm closely matching the Excel API.
    Sharing one session for subsequent requests is faster, however it is not thread-safe, as some events can come faster than others.
    bdp returns a string, bdh returns a pandas DataFrame.
    This is mostly useful for scripting, but care should be taken when used in a real world application.
    """

    def __init__(self):
        self.session = blpapi.Session()
        self.session.start()
        self.session.openService('//BLP/refdata')
        self.refDataSvc = self.session.getService('//BLP/refdata')
        self.session.openService("//blp/exrsvc")
        self.refExrSvc = self.session.getService('//BLP/exrsvc')

    def bdp(self, strSecurity='US900123AL40 Govt', strData='PX_LAST',
            strOverrideField='', strOverrideValue='', strOverrideField2='', strOverrideValue2='', ):
        request = self.refDataSvc.createRequest('ReferenceDataRequest')
        request.append('securities', strSecurity)
        request.append('fields', strData)

        if strOverrideField != '':
            o = request.getElement('overrides').appendElement()
            o.setElement('fieldId', strOverrideField)
            o.setElement('value', strOverrideValue)

        if strOverrideField2 != '':
            o2 = request.getElement('overrides').appendElement()
            o2.setElement('fieldId', strOverrideField2)
            o2.setElement('value', strOverrideValue2)


        requestID = self.session.sendRequest(request)

        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break
        try:
            output = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA).getValueAsElement(0).getElement(FIELD_DATA).getElementAsString(strData)
            if output == '#N/A':
                output = 'None'
        except:
            print('error with '+strSecurity+' '+strData)
            output = 'Error with api data field, i.g PX_LAST'
        return output

    def bdh(self, strSecurity='SPX Index', strData='PX_LAST', startdate=datetime.date(2014, 1, 1), enddate=datetime.date(2014, 1, 9), adjustmentSplit=False, periodicity='DAILY', singleFrame=False):
        request = self.refDataSvc.createRequest('HistoricalDataRequest')
        request.append('securities', strSecurity)
        if type(strData) == str:
            strData = [strData]

        for strD in strData:
            request.append('fields', strD)

        request.set('startDate', startdate.strftime('%Y%m%d'))
        request.set('endDate', enddate.strftime('%Y%m%d'))
        request.set('adjustmentSplit', 'TRUE' if adjustmentSplit else 'FALSE')
        request.set('periodicitySelection', periodicity)
        requestID = self.session.sendRequest(request)

        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break

        fieldDataArray = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA).getElement(FIELD_DATA)
        fieldDataList = [fieldDataArray.getValueAsElement(i) for i in range(0, fieldDataArray.numValues())]
        outDates = [x.getElementAsDatetime(DATE) for x in fieldDataList]
        output = pandas.DataFrame(index=outDates, columns=strData)

        for strD in strData:
            output[strD] = [x.getElementAsFloat(strD) for x in fieldDataList]

        output.replace('#N/A History', pandas.np.nan, inplace=True)
        output.index = pandas.to_datetime(output.index)

        if singleFrame == True:
            x = output[strData].values.tolist()
            lastPrice = x[-1]
            
            return lastPrice
        else:
            return output


    def bsrch(self, domain):
        '''
        Used to retrieve results from SRCH function in terminal.
        If you save a search called eg EX, calling bsrch("fi:EX") will return its results.
        '''
        request = self.refExrSvc.createRequest('ExcelGetGridRequest')
        request.set('Domain', domain)
        requestID = self.session.sendRequest(request)

        while True:
            event = self.session.nextEvent()
            if event.eventType() == blpapi.event.Event.RESPONSE:
                break
        data = []
        for msg in event:
            for v in msg.getElement("DataRecords").values():
                for f in v.getElement("DataFields").values():
                    data.append(f.getElementAsString("StringValue"))
        
        return pandas.DataFrame(data)

    def bdhOHLC(self, strSecurity='SPX Index', startdate=datetime.date(2014, 1, 1), enddate=datetime.date(2014, 1, 9), periodicity='DAILY'):
        return self.bdh(strSecurity, ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST'], startdate, enddate, periodicity)



    def closeSession(self):
        self.session.stop()
	
################################################


class BLPTS():
    """Thread-safe implementation of the Request/Response Paradigm.
    The functions don't return anything but notify observers of results.
    Including startDate as a keyword argument will define a HistoricalDataRequest, otherwise it will be a ReferenceDataRequest.
    HistoricalDataRequest sends observers a pandas DataFrame, whereas ReferenceDataRequest sends a pandas Series.
    Override seems to only work when there's one security, one field, and one override.
    Examples:
    BLPTS(['ESA Index', 'VGA Index'], ['BID', 'ASK'])
    BLPTS('US900123AL40 Govt','YLD_YTM_BID',strOverrideField='PX_BID',strOverrideValue='200')
    BLPTS(['SPX Index','SX5E Index','EUR Curncy'],['PX_LAST','VOLUME'],startDate=datetime.datetime(2014,1,1),endDate=datetime.datetime(2015,5,14),periodicity='DAILY')
    """

    def __init__(self, securities=[], fields=[], **kwargs):
        """
        Keyword arguments:
        securities : list of ISINS 
        fields : list of fields 
        kwargs : startDate and endDate (datetime.datetime object, note: hours, minutes, seconds, and microseconds must be replaced by 0)
        """
        self.session    = blpapi.Session()
        self.session.start()
        self.session.openService('//BLP/refdata')
        self.refDataSvc = self.session.getService('//BLP/refdata')
        self.observers  = []
        self.kwargs     = kwargs

        if len(securities) > 0 and len(fields) > 0:
            # also works if securities and fields are a string
            self.fillRequest(securities, fields, **kwargs)

    def fillRequest(self, securities, fields, **kwargs):
        """
        keyword arguments:
        securities : list of ISINS
        fields : list of fields 
        kwargs : startDate and endDate (datetime.datetime object, note: hours, minutes, seconds, and microseconds must be replaced by 0)
        """
        self.kwargs = kwargs

        if type(securities) == str:
            securities = [securities]

        if type(fields) == str:
            fields = [fields]

        if 'startDate' in kwargs:
            self.request   = self.refDataSvc.createRequest('HistoricalDataRequest')
            self.startDate = kwargs['startDate']
            self.endDate   = kwargs['endDate']

            if 'periodicity' in kwargs:
                self.periodicity = kwargs['periodicity']
            else:
                self.periodicity = 'DAILY'

            self.request.set('startDate', self.startDate.strftime('%Y%m%d'))
            self.request.set('endDate', self.endDate.strftime('%Y%m%d'))
            self.request.set('periodicitySelection', self.periodicity)

        else:
            self.request = self.refDataSvc.createRequest('ReferenceDataRequest')
            self.output  = pandas.DataFrame(index=securities, columns=fields)

            if 'strOverrideField' in kwargs:
                o = self.request.getElement('overrides').appendElement()
                o.setElement('fieldId', kwargs['strOverrideField'])
                o.setElement('value', kwargs['strOverrideValue'])

        self.securities = securities
        self.fields     = fields

        for s in securities:
            self.request.append('securities', s)

        for f in fields:
            self.request.append('fields', f)

    def get(self, newSecurities=[], newFields=[], **kwargs):
        """
        securities : list of ISINS 
        fields : list of fields 
        kwargs : startDate and endDate (datetime.datetime object, note: hours, minutes, seconds, and microseconds must be replaced by 0)
        """

        if len(newSecurities) > 0 or len(newFields) > 0:
            self.fillRequest(newSecurities, newFields, **kwargs)

        self.requestID = self.session.sendRequest(self.request)

        while True:
            event = self.session.nextEvent()
            if event.eventType() in [blpapi.event.Event.RESPONSE, blpapi.event.Event.PARTIAL_RESPONSE]:
                responseSize = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA).numValues()

                for i in range(0, responseSize):

                    if 'startDate' in self.kwargs:
                        # HistoricalDataRequest
                        output         = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA)
                        security       = output.getElement(SECURITY).getValueAsString()
                        fieldDataArray = output.getElement(FIELD_DATA)
                        fieldDataList  = [fieldDataArray.getValueAsElement(i) for i in range(0, fieldDataArray.numValues())]
                        dates          = map(lambda x: x.getElement(DATE).getValueAsString(), fieldDataList)
                        outDF          = pandas.DataFrame(index=dates, columns=self.fields)
                        outDF.index    = pandas.to_datetime(outDF.index)

                        for field in self.fields:
                            data = []
                            for row in fieldDataList:
                                if row.hasElement(field):
                                    data.append(row.getElement(field).getValueAsFloat())
                                else:
                                    data.append(pandas.np.nan)

                            outDF[field] = data
                            self.updateObservers(security=security, field=field, data=outDF) # update one security one field

                        self.updateObservers(security=security, field='ALL', data=outDF) # update one security all fields

                    else:
                        # ReferenceDataRequest
                        output   = blpapi.event.MessageIterator(event).next().getElement(SECURITY_DATA).getValueAsElement(i)
                        n_elmts  = output.getElement(FIELD_DATA).numElements()
                        security = output.getElement(SECURITY).getValueAsString()
                        for j in range(0, n_elmts):
                            data     = output.getElement(FIELD_DATA).getElement(j)
                            field    = str(data.name())
                            outData  = _dict_from_element(data)
                            self.updateObservers(security=security, field=field, data=outData) # update one security one field
                            self.output.loc[security, field] = outData
                            
                        if n_elmts>0:
                            self.updateObservers(security=security, field='ALL', data=self.output.loc[security]) # update one security all fields
                        else:
                            print('Empty response received for ' + security)

            if event.eventType() == blpapi.event.Event.RESPONSE:
                break

    def register(self, observer):
        if not observer in self.observers:
            self.observers.append(observer)

    def unregister(self, observer):
        if observer in self.observers:
            self.observers.remove(observer)

    def unregisterAll(self):
        if self.observers:
            del self.observers[:]

    def updateObservers(self, *args, **kwargs):
        for observer in self.observers:
            observer.update(*args, **kwargs)

    def closeSession(self):
        self.session.stop()

def main():
    pass

if __name__ == '__main__':
    main()

