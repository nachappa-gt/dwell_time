#!/usr/bin/perl
#
# Copyright (C) 2014.  xAD, Inc.  All Rights Reserved.
#
=pod

=head1 NAME

xcp - xAd tool for downlading data to HDFS from various sources.

=head1 SYNOPSIS

xcp cmd [options]

=head1 DESCRIPTION

This tool download data to HDFS from verious sources, such as
Amazon S3 and  MySQL.

For S3, it uses the Hadoop tool B<distcp>.  
For MySQL, it uses B<sqoop>.

Default options are defined in the configueration files under

    /home/xad/xcp/config/


=head1 COMMANDS

A complete list of commands and alises can be found in

    /home/xad/xcp/config/commands.properties

=head2 Campaign Hourly Summary

=over 4

=item B<chs-get>

Download hourly campaign data summary files.
The source is located in S3 under:

    s3://enigma-dw2/facts/campaign_hourly_summary

HDFS location is:

    /data/redshift/campaign_hourly_summary

=item B<chs-clean>

Remove old campaign hourly summary files from HDFS.

=back

=head2 Enigma

=over 4

=item B<enigma-daily>

Download enigma logs at the daily level.
The source is in S3 under:

    s3://enigma-data/raw-data/camus/data
    s3://enigma-data-backup/raw-data/camus/data

HDFS location is

    /data/enigma

=item B<enigma-hourly>

Download enigma logs at the hourly level.
Output location is /data/enigma.

=item B<enigma-clean>

Delete older data.
The associated property is B<enigma.keep.window>,
which can be overriden with the B<--date> option.

=item B<enigma-ls>

List the lastest copied dates and hours.

=back

=head2 XADCMS

=over 4

=item B<xadcms-get>

Download selected tables from XADCMS.
A complete list of tables can be found in the
B<xadcms.mysql.tables> property in the configuration.
The HDFS location is

    /data/xadcms.

=item B<xadcms-clean>

Remove older XADCMS files from HDFS.

=back


=head1 OPTIONS

=over 4


=item B<--date> date(s)

Override the default dates specified in the configuration.
It takes one or many dates delimited by commas.
It also takes date ranges delimited by ":".
There are also some special dates such as
"L5" means the last 5 days;
"L5" means the last 5 days ofsset by 1 day.
For example,

  --date 2015-01-23
  --date 2015-01-23,2015-01-25
  --date 2015-01-23:2015-01-31
  --date 2015-01
  --date L5
  --date L5-1
  --date yesterday

=item B<--hour> hour(s)

Specify hours, which can be a list of hours delimited
by commas, or a range of hours delimited by ":".  E.g

  --hour 1
  --hour 1,3,5
  --hour 1:10
  --hour even
  --hour odd

=item B<--country> countries

Specify the countries, such as
us, gb, ca, cn, in, de.

=item B<--logtype> logtypes(s)

Specify the desired logtypes.
Supported values are exchange, display, and search.
Multiple logtypes are delimited by commas.

=item B<--event> enigma_event_type(s)

Specify the enigma event types, such as 
AdRequest, AdDetails, AdTracking, AdUserProfile, and HttpVendorStats.
Multiple event types are delimited by commas.

=item B<--debug> OR B<-d>

Turn on the debug option, which will print more messages.

=item B<--force> OR B<-f>

For a command to be run.
Some commands will check the local status logs or the
files in HDFS.   They will not reprocess the data
if they are already downloaded.
This option will force them to run regardless of the
download status.

=item B<--maxmaps> OR B<-m>

Specify the maximum number of maps to be assigned for
a B<distcp> related commands (such as 
B<enigma-daily>, B<enigma-hourly>, B<chs-get>).
Default is 5.  This option can be turned off with a value of 0;
thus, letting B<distcp> use its own default.

=item B<--norun> OR B<-n>

Show the indended actions without actually running them.

=item B<--noemail> OR B<-ne>

Stop the email notification on exceptions.

=back

=cut


use strict;
use xad::CommandKey;
use xad::copy::Xcp;

use Getopt::Long;
use MIME::Lite;
use Sys::Hostname;

my $PROMPT = "[xcp.pl]";
my $DEFAULT_CONFIG_PATH = "/home/xad/xcp/config:/home/xad/share/config";
my $DEFAULT_CONFIG_FILE = "xcp.properties";
my $EMAIL = 'victor.chu@xad.com';
my $EMAIL_PRIORITY = 0;
my $HOSTNAME = hostname();

#
# Print the usage information
#
sub usage() {
    system("perldoc", $0);
    exit (1);
}


#
# Define command alias and keys.  Single commands need to be
# listed in the desired execution order.
#

#
# Exception handler.
# 
sub errorHandler($$$@) {
    my $to_email = shift;
    my $NORUN = shift;
    my $priority = shift;
    my @msg = @_;

    print "\n$PROMPT EXCEPTION: @msg\n";

    my $host = $HOSTNAME;
    my $email = MIME::Lite->new(
        To      => join(", ", split(/[\s,]+/, $to_email)),
        Subject => "[ALERT] XCP ($host)",
        Type    => "text/plain",
        Data    => "MESSAGE:\n@msg"
    );
    if ($priority) {
        $email->add('X-Priority' => $priority);
    }
    unless ($NORUN) {
        print "$PROMPT Sending alert email to '$to_email' (priority = $priority)...\n";
        $email->send;
    }
}


# Print the time stamp
sub timestamp() {
    print "#---------------------------------\n";
    print "# " . `date`;
    print "#---------------------------------\n";
}


# Process for one config and a set of arguments
sub process($$$$)
{
    my $opt = shift;
    my $config = shift;
    my $args = shift;
    my $info = shift;

    print "\n";
    print "$PROMPT ===========================================================\n";
    print "$PROMPT  Processing '$config' : [@$args]\n";
    print "$PROMPT ===========================================================\n\n";

    # Initialize Spring
    my $xcp = xad::copy::Xcp->new();
    $xcp->loadConfig($opt->{'config_path'}, $config, $opt->{'debug'});
    $xcp->mergeOptions($opt);
    #$xcp->printConfig();

    # Global variables
    $EMAIL = $xcp->getConfig('alert.email');
    $EMAIL_PRIORITY = $xcp->getConfig('alert.email.priority', 0);
    $HOSTNAME = $xcp->getConfig('local.host.id', $HOSTNAME);

    # Execute the command
    my $cmdKey = xad::CommandKey->new(CFG => $xcp->{CFG});
    $cmdKey->activateCommands(@$args);
    #$cmdKey->print();

    # Locking
    if (! $opt->{'force'}) {
        my $locktype = $opt->{'locktype'} ? $opt->{'locktype'} : "";
        if (! $xcp->lock($locktype)) {
            my $lockfile = $xcp->getLockFile($locktype);
            warn "WARNING - failed to lock '$lockfile'\n";
            return;
        }
    }

    # Fork Manager
    $xcp->createForkManager($opt->{'fork'} ? $opt->{'fork'} : 0);

    # Process Commands
    my @commands = $cmdKey->getActiveKeys();
    foreach my $cmd (@commands) {
        timestamp();
        print "$PROMPT ----------------------------------------------------------\n";
        print "$PROMPT  Command '$cmd'\n";
        print "$PROMPT ----------------------------------------------------------\n";
        push @$info, "  + $cmd ...";

        # Initialization
        if ($cmd eq $cmdKey->get('init')) {
            $xcp->initHDFS();
        }

        # Enigma (S3)
        elsif ($cmd eq $cmdKey->get('enigma-daily')) {
            $xcp->enigmaGetDaily();
        }
        elsif ($cmd eq $cmdKey->get('enigma-hourly')) {
            $xcp->enigmaGetHourly();
        }
        elsif ($cmd eq $cmdKey->get('enigma-du')) {
            $xcp->enigmaDiskUsage();
        }
        elsif ($cmd eq $cmdKey->get('enigma-validate')) {
            $xcp->enigmaValidate();
        }
        elsif ($cmd eq $cmdKey->get('enigma-sync-log-status')) {
            $xcp->enigmaSyncLogStatus();
        }
        elsif ($cmd eq $cmdKey->get('enigma-status-log')) {
            $xcp->enigmaStatusLog();
        }
        elsif ($cmd eq $cmdKey->get('enigma-clean')) {
            $xcp->enigmaClean();
        }
        elsif ($cmd eq $cmdKey->get('enigma-clean-nf')) {
            $xcp->enigmaCleanNoFill();
        }
        elsif ($cmd eq $cmdKey->get('enigma-ls')) {
            $xcp->enigmaLs();
        }

        # RedShift
        elsif ($cmd eq $cmdKey->get('rs-dim-get')) {
            $xcp->redshiftGetDimensions();
        }
        elsif ($cmd eq $cmdKey->get('rs-dim-archive')) {
            $xcp->redshiftArchiveDimensions();
        }
        elsif ($cmd eq $cmdKey->get('rs-dim-clean')) {
            $xcp->redshiftCleanDimensions();
        }

        # XADCMS (MySQL)
        elsif ($cmd eq $cmdKey->get('xadcms-get')) {
            $xcp->xadcmsGet();
        }
        elsif ($cmd eq $cmdKey->get('xadcms-clean')) {
            $xcp->xadcmsClean();
        }

        # POI (PostgreSQL)
        elsif ($cmd eq $cmdKey->get('poidb-get')) {
            $xcp->poidbGet();
        }
        elsif ($cmd eq $cmdKey->get('poidb-split')) {
            $xcp->poidbSplit();
        }
        elsif ($cmd eq $cmdKey->get('poidb-clean')) {
            $xcp->poidbClean();
        }

        # Campaign Hourly Summary (S3)
        elsif ($cmd eq $cmdKey->get('camp-hourly-summary-get')) {
            $xcp->campaignHourlyGet();
        }
        elsif ($cmd eq $cmdKey->get('camp-hourly-summary-clean')) {
            $xcp->campaignHourlyClean();
        }

        # RTB Hourly Summary (S3)
        elsif ($cmd eq $cmdKey->get('rtb-hourly-summary-get')) {
            $xcp->rtbHourlyGet();
        }
        elsif ($cmd eq $cmdKey->get('rtb-hourly-summary-clean')) {
            $xcp->rtbHourlyClean();
        }

        # Extract (S3)
        elsif ($cmd eq $cmdKey->get('extract-daily')) {
            $xcp->extractGetDaily();
        }
        elsif ($cmd eq $cmdKey->get('extract-hourly')) {
            $xcp->extractGetHourly();
        }
        elsif ($cmd eq $cmdKey->get('extract-du')) {
            $xcp->extractDiskUsage();
        }
        elsif ($cmd eq $cmdKey->get('extract-validate')) {
            $xcp->extractValidate();
        }
        elsif ($cmd eq $cmdKey->get('extract-status-log')) {
            $xcp->extractStatusLog();
        }
        elsif ($cmd eq $cmdKey->get('extract-clean')) {
            $xcp->extractClean();
        }
        elsif ($cmd eq $cmdKey->get('extract-clean-nf')) {
            $xcp->extractCleanNoFill();
        }
        elsif ($cmd eq $cmdKey->get('extract-ls')) {
            $xcp->extractLs();
        }

        # Tmp Folder Cleaning (HDFS)
        elsif ($cmd eq $cmdKey->get('clean-tmp')) {
            $xcp->cleanTmp();
        }
        elsif ($cmd eq $cmdKey->get('clean-sub-tmp')) {
            $xcp->cleanSubTmp();
        }
        elsif ($cmd eq $cmdKey->get('clean-user')) {
            $xcp->cleanUser();
        }
        elsif ($cmd eq $cmdKey->get('clean-logs')) {
            $xcp->cleanLogs();
        }
        elsif ($cmd eq $cmdKey->get('clean-dir')) {
            $xcp->cleanDirs();
        }

        # TEST
        elsif ($cmd eq $cmdKey->get('test')) {
            $xcp->test();
        }

        # Unkonwn
        else {
            die "Unhandled command '$cmd'";
        }
    }
    $xcp->waitAllChildren();
    $xcp->unlock();
}

#
# The main function
#
sub main()
{
    my @info = ("---", "CMD = $0 " . join(" ", @ARGV));

    # Set default options
    my %opt = (
        config_path => $DEFAULT_CONFIG_PATH,
        config  => $DEFAULT_CONFIG_FILE,
    );

    # Read configuration into the hash table.
    my $ok = GetOptions(\%opt,
        "config|c=s",
        "config_path|c=s",
        "country=s",
        "logtype=s",
        "maxmaps|m=i",
        "event=s",
        "date=s",
        "debug|d",
        "dir=s",
        "hour=s",
        "force|f",
        "fork=i",
        "help",
        "hour=s",
        "locktype=s",
        "format=s",     # data format for XADCMS
        "noemail|ne",
        "norun|n",
        "offset=i",     # status log offset for data request
        "queue|q=s",    # job queue
        "update",
        "window=i",     # clean window
    );

    if ($opt{'help'} || @ARGV == 0) {
        usage();
    }

    eval {
        my $config = $opt{'config'};
        push @info, "Processing $config:[@ARGV] ...";
        process(\%opt, $config, \@ARGV, \@info);
    };
    if ($@) {
        my $msg = join("\n", $@, @info);
        errorHandler($EMAIL, $opt{norun} || $opt{noemail}, $EMAIL_PRIORITY, $msg);
    }
    timestamp();
}

main();


