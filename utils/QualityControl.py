#! /bin/env python3
from utils.SendEmail import SendEmail

class QualityControl():
    """ A set of fucntions for Quality Control:

            1. QC for flowcell
            2. QC for sample
            3. QC for variants
    """

    def __init__(self, conn):
        self.flowcell = conn.Execute("SELECT * from qcMetricsMachine") 
        self.sample   = conn.Execute("SELECT * from qcMetricsSample") 
        self.variant  = conn.Execute("SELECT * from qcMetricsVariant") 

    def compareMultipleFields(self, field, conditions, values):
        for vals in values:
           for equation in conditions.split(' && '):
               if not eval( str(vals) + " " + equation ) :
                   return "One of the %s (Value: %s) failed to pass the filter (%s).\n" % (field, ",".join(str(x) for x in values), conditions)
        return ""

    def qc4flowcell(self, flowcellid, machine, infors):
        emailcontent = ""
        for row in self.flowcell:
            if row['machineType'] != machine:
                continue
            emailcontent += self.compareMultipleFields(row['FieldName'], row['Value'], infors[row['FieldName']])
        return emailcontent 

    def qc4sample(self, sampleid, machine, captureKit, infors, level):
        emailcontent = ""
        for row in self.sample:
            if row['machineType'] != machine or row['level'] != level or row['captureKit'] != captureKit or row['FieldName'] not in infors:
                continue
            emailcontent += self.compareMultipleFields(row['FieldName'], row['Value'], [infors[row['FieldName']]])
        return emailcontent 

    def qc4variant(self):
        pass

