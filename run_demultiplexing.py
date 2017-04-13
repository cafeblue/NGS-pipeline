#! /bin/env python3
"""
This script is to find the finished runfolders on the sequencers and run demultiplexing.
"""
import sys
import datetime
import os
import re
import pickle
from pathlib import Path
import subprocess
from utils import *

from interop import py_interop_run_metrics, py_interop_run, py_interop_summary


class Usage:
    """
        Find the finished runfolders on the sequencers and run demultiplexing.

        Usage: python3 rsyncIlmnRunDir.py database.cnf
        Example: python3 rsyncIlmnRunDir.py clinicalB.cnf

    """

def getSequencingList(config, conn):
    runningSeqs = conn.Execute('SELECT flowcellID,machine,destinationDir,cycleNum,LaneCount,SurfaceCount,SwathCount,TileCount,SectionPerLane,LanePerSection from thing1JobStatus where sequencing ="2"')
    if len(runningSeqs) > 0 :
        flag = 0
        finished_seqs = list()
        for runningSeq in runningSeqs:
            if check_status(runningSeq, config) == 1:
                finished_seqs.append(dict(runningSeq))
                flag += 1
        if flag > 0:
            return finished_seqs

    sys.exit(0)

def check_status(runningSeq, config):
    runningSeq['destinationDir'] = re.sub(r'wei.wang@data1.ccm.sickkids.ca:', '', runningSeq['destinationDir'])
    if Path(runningSeq['destinationDir'] + eval(getattr(config, "LAST_BCL_" + re.sub(r'_.{1,2}$', '', runningSeq['machine'])))).is_file():
        completeFile = runningSeq['destinationDir'] + getattr(config, "COMPLETE_FILE_" + re.sub(r'_.{1,2}$', '', runningSeq['machine']))
        if Path(completeFile).is_file() :
            if (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(completeFile))).seconds > 600:
                return 1
    return 2

def create_sample_sheet():
'''
sub create_sample_sheet {
  my ($machine, $flowcellID, $cycle1, $cycle2) = @_;
  my $machineType = $machine;
  $machineType =~ s/_.+//;
  my $errlog = "";
  my @old_samplesheet = ();

  my $filename = "$config->{'SAMPLE_SHEET'}$machine\_$flowcellID.csv";
  if ( -e "$filename" ) {
    $errlog .= "samplesheet already exists: $filename\n";
    @old_samplesheet = `tail -n +2  $filename`;
  }

  my $csvlines = "";
  my $db_query = "SELECT sampleID,barcode,lane,barcode2 from sampleSheet where flowcell_ID = \'$flowcellID\'" ;
  my $sthQNS = $dbh->prepare($db_query) or die "Can't query database for new samples: ". $dbh->errstr() . "\n";
  $sthQNS->execute() or die "Can't execute query for new samples: " . $dbh->errstr() . "\n";
  if ($sthQNS->rows() != 0) { #no samples are being currently sequenced
    $csvlines .= eval($config->{"SAMPLESHEET_HEADER_$machineType"}); 
    if ($config->{"SAMPLESHEET_HEADER_$machineType"} =~ /Lane/) {
      while (my @data_line = $sthQNS->fetchrow_array()) {
        foreach my $lane (split(/,/, $data_line[2])) {
          $csvlines .= eval($config->{"SAMPLESHEET_LINE_$machineType"}); 
        }
      }
    }
    else {
      while (my @data_line = $sthQNS->fetchrow_array()) {
        $csvlines .= eval($config->{"SAMPLESHEET_LINE_$machineType"}); 
      }
    }
  } else {
    Common::email_error($config->{"EMAIL_SUBJECT_PREFIX"}, $config->{"EMAIL_CONTENT_PREFIX"}, "$machine $flowcellID demultiplex status","No sampleID could be found for $flowcellID in the database, table sampleSheet", $machine, $today, $flowcellID, $config->{'EMAIL_WARNINGS'});
    croak "no sample could be found for $flowcellID \n";
  }

  my $check_ident = 0;
  if ($#old_samplesheet > -1) {
    my %test;
    foreach (@old_samplesheet) {
      chomp;
      $test{$_} = 0;
    }
    foreach (split(/\n/,$csvlines)) {
      if (not exists $test{$_}) {
        $errlog .= "line\n$_\ncan't be found in the old samplesheet!\n";
        $check_ident = 1;
      }
    }
  }

  if ($check_ident == 1) {
    Common::email_error($config->{"EMAIL_SUBJECT_PREFIX"}, $config->{"EMAIL_CONTENT_PREFIX"}, "$machine $flowcellID demultiplex status",$errlog, $machine, $today, $flowcellID, $config->{'EMAIL_WARNINGS'});
    croak $errlog;
  } elsif ($check_ident == 0 && $errlog ne '') {
    Common::email_error($config->{"EMAIL_SUBJECT_PREFIX"}, $config->{"EMAIL_CONTENT_PREFIX"}, "$machine $flowcellID demultiplex status",$errlog, $machine, $today, $flowcellID, $config->{'EMAIL_WARNINGS'});
    return $filename;
  }

  open (CSV, ">$filename") or die "failed to open file $filename";
  print CSV eval($config->{'SEQ_SAMPLESHEET_INFO'}),"\n"; 
  print CSV $csvlines;
  return $filename;
}
'''


def main(name, dbfile):

    if Path(dbfile).is_file():
        pass
    else:
        print("ERR: file: " + str(dbfile) + " does not exists!")
        print(Usage.__doc__)
        sys.exit(2)

    conn = DB_Connector(dbfile)
    config = GlobalConfig(conn)
    cron_control = CronControlPanel(conn)
    ilmnbarcode = ilmnBarCode(conn)

    finishedSeqs = getSequencingList(config, conn)

    timestamp = TimeString()
    timestamp.print_timestamp()
    for runningSeq in finishedSeqs:
        ssRows = conn.Execute("SELECT * FROM sampleSheet WHERE flowcell_ID = '%s'" % (runningSeq['flowcellID'])) 
        samplesheet = SampleSheet(ssRows, conn, timestamp)
        samplesheet.demultiplex_samplesheet()
        command = "bcl2fastq -R %s -o $outputfastqDir --sample-sheet %s" % (runningSeq['destinationDir'], getattr(config, "FASTQ_FOLDER") , samplesheet.samplSheetFile)
        jobID = jsub("demultiplex", command, "540000", "32", '1', "01:00:00", "30", '', "bcl2fastq/2.19.0")

        ###  interOp
        run_folder = runningSeq['destinationDir']
        run_metrics = py_interop_run_metrics.run_metrics()
        valid_to_load = py_interop_run.uchar_vector(py_interop_run.MetricCount, 0)
        run_folder = run_metrics.read(run_folder, valid_to_load)
        summary = py_interop_summary.run_summary()
        py_interop_summary.summarize_run_metrics(run_metrics, summary)
        rows = [("Read %s%d"%("(I)" if summary.at(i).read().is_index()  else " ", summary.at(i).read().number()), summary.at(i).summary()) for i in xrange(summary.size())]
        d = []
        for label, func in columns:
            d.append( (label, pd.Series([getattr(r[1], func)() for r in rows], index=[r[0] for r in rows])))
        df = pd.DataFrame.from_items(d)
        df

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print(Usage.__doc__)
        sys.exit(2)

    main(*sys.argv)
