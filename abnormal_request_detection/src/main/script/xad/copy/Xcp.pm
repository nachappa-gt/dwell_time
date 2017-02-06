#
# Copyright (C) 2014.  xAd, Inc.  All rights reserved.
#
=pod

=head1 NAME

xad::copy::Xcp.pm

=head1 DESCRIPTION

Main module for xcp.  It deligates tasks to other sub-modules.

=cut

package xad::copy::Xcp;

use strict;
use warnings;

use xad::Conf;
use xad::DateUtil;
#use xad::DirList;
use xad::ForkManager;
use xad::HDFS;
use xad::S3;
use xad::StatusLog;
use xad::RedShiftUtil;

use xad::copy::BaseXcp;
use xad::copy::CampaignXcp;
use xad::copy::EnigmaXcp;
use xad::copy::RedShiftXcp;
use xad::copy::RtbXcp;
use xad::copy::XadcmsXcp;
use xad::copy::PoidbXcp;
use xad::copy::Clean;
use xad::copy::ExtractXcp;

use Fcntl qw(:flock);
use IO::LockedFile ( block => 0 );

use base qw(xad::copy::BaseXcp);

my $PROMPT = "[Xcp.pm]";
my $DEFAULT_CONFIG_FILE = "xcp.properties";


#-----------------
# Constructor
#-----------------
sub new(@) {
    my $classname = shift;
    die "class method called on object" if ref $classname;

    my $opt = {@_};
    my $self = xad::copy::BaseXcp->new(@_);
    bless($self, $classname);
    $self->_init($opt);
    return $self;
}

# COMMON
sub _init($) {
    my $self = shift;
    my $opt = shift;

    # Supporting modules
    $self->{CFG}  = xad::Conf->new();
    $self->{FM}   = xad::ForkManager->new();
    $self->{HDFS} = xad::HDFS->new();
    $self->{S3}   = xad::S3->new();
    $self->{SL}   = xad::StatusLog->new(CFG => $self->{CFG});
    $self->{RSU}  = xad::RedShiftUtil->new(CFG => $self->{CFG});

    my %context = (
        CFG  => $self->{CFG},
        FM   => $self->{FM},
        HDFS => $self->{HDFS},
        S3   => $self->{S3},
        SL   => $self->{SL},
        RSU  => $self->{RSU},
    );

    # Create target modules
    $self->{CAMP}   = xad::copy::CampaignXcp->new(%context);
    $self->{CLEAN}  = xad::copy::Clean->new(%context);
    $self->{ENIGMA} = xad::copy::EnigmaXcp->new(%context);
    $self->{RTB}    = xad::copy::RtbXcp->new(%context);
    $self->{RS}     = xad::copy::RedShiftXcp->new(%context);
    $self->{XADCMS} = xad::copy::XadcmsXcp->new(%context);
    $self->{POIDB}  = xad::copy::PoidbXcp->new(%context);
    $self->{EXTRACT}= xad::copy::ExtractXcp->new(%context);

}


#--------------------
# Locking
#--------------------
# Get the lock file path
sub getLockFile(@) {
    my $self = shift;
    my $locktype = @_ ? shift : "";

    my $lockFile = $self->{CFG}->get('lock.file');
    $lockFile .= ".$locktype" if ($locktype);

    # Create the directory if it is missing
    if ($lockFile =~ /\//) {
        my $dir = $lockFile;
        $dir =~ s/[^\/]+$//;
        if (! -d $dir) {
            (mkdir $dir) || die "ERROR mkdir $dir";
        }
    }
    return $lockFile;
}

# Lock a file
sub lock(@) {
    my $self = shift;
    my $lockfile = $self->getLockFile(@_);
    print "$PROMPT Locking $lockfile...\n" if $self->{DEBUG};
    my $fh = new IO::LockedFile("> $lockfile");
    $self->{LOCKFILE}   = $lockfile;
    $self->{LOCKHANDLE} = $fh;
    return $fh;
}

# Unlock the file
sub unlock($) {
    my $self = shift;
    my $fh = $self->{LOCKHANDLE};
    if ($fh) {
        my $lockfile = $self->{LOCKFILE};
        print "\n$PROMPT Unlock $lockfile\n" if $self->{DEBUG};
        $fh->close();
    }
}


#--------------------
# configuration
#--------------------
sub loadConfig($@) {
    my $self = shift;
    my $path = shift;
    my $configFile = @_ ? shift : $DEFAULT_CONFIG_FILE;
    my $debug = @_ ? shift : $self->{DEBUG};

    print "$PROMPT loadConfig($path, $configFile)\n" if $self->{DEBUG};

    $self->{CFG}->{DEBUG} = $debug;
    $self->{CFG}->load($path, $configFile);
}


sub overrideConfig($) {
    my $self = shift;
    my $opt = shift;

    my $cfg = $self->{CFG};
    my $hdfs = $self->{HDFS};

    # Copy opt values to self
    while (my ($key,$val) = each %$opt) {
        $key = uc $key;
        $self->{$key} = $val;
    }

    # Override sub components
    $self->{HDFS}->overrideConfig($opt);
    #$self->{DP}->overrideConfig($opt);
}


sub getConfig($) {
    my $self = shift;
    return $self->{CFG}->get(@_);
}

sub printConfig() {
    my $self = shift;
    $self->{CFG}->print();
}

# Pass command line arguments to components
sub mergeOptions(@) {
    my $self = shift;
    my $opt = shift;
    $self->xad::copy::BaseXcp::mergeOptions($opt);
}


#-----------
# Fork
#-----------
sub createForkManager(@) {
    my $self = shift;
    $self->{FM}->create(@_);
}

sub waitAllChildren(@) {
    my $self = shift;
    $self->{FM}->waitAllChildren(@_);
}


#--------------------------
# Campaign Hourly Summary
#--------------------------
sub campaignHourlyGet() {
    my $self = shift;
    $self->{CAMP}->getHourly();
}

sub campaignHourlyClean() {
    my $self = shift;
    $self->{CAMP}->cleanFiles();
}


#--------------------------
# RTB Hourly Summary
#--------------------------
sub rtbHourlyGet() {
    my $self = shift;
    $self->{RTB}->getHourly();
}

sub rtbHourlyClean() {
    my $self = shift;
    $self->{RTB}->cleanFiles();
}


#-----------
# Enigma
#-----------
sub enigmaGetDaily() {
    my $self = shift;
    $self->{ENIGMA}->getDailyFiles();
}

sub enigmaDiskUsage() {
    my $self = shift;
    $self->{ENIGMA}->getDiskUsage();
}

sub enigmaValidate() {
    my $self = shift;
    $self->{ENIGMA}->validate();
}

sub enigmaClean() {
    my $self = shift;
    $self->{ENIGMA}->cleanFiles();
}

sub enigmaCleanNoFill() {
    my $self = shift;
    $self->{ENIGMA}->cleanNoFillFiles();
}

sub enigmaGetHourly() {
    my $self = shift;
    $self->{ENIGMA}->getHourlyFiles();
}

sub enigmaSyncLogStatus() {
    my $self = shift;
    $self->{ENIGMA}->syncLogStatus();
}

sub enigmaStatusLog() {
    my $self = shift;
    $self->{ENIGMA}->displayStatusLog();
}

sub enigmaLs() {
    my $self = shift;
    $self->{ENIGMA}->listLocal();
}


#-------------------
# XADCMS
#-------------------
sub xadcmsGet() {
    my $self = shift;
    $self->{XADCMS}->getTables();
}

sub xadcmsClean() {
    my $self = shift;
    $self->{XADCMS}->cleanTables();
}


#-------------------
# POI DB
#-------------------
sub poidbGet() {
    my $self = shift;
    $self->{POIDB}->getTables();
}

sub poidbSplit() {
    my $self = shift;
    $self->{POIDB}->splitTables();
}

sub poidbClean() {
    my $self = shift;
    $self->{POIDB}->cleanTables();
}


#-------------------------------
# RedShift
#-------------------------------
# Call tracking logs
sub ctlGetS3() {
    my $self = shift;
    $self->{RS}->ctlGetS3();
}

sub ctlGetHDFS() {
    my $self = shift;
    $self->{RS}->ctlGetHDFS();
}

# Get Dimension Tables
sub redshiftGetDimensions() {
    my $self = shift;
    $self->{RS}->getDimensions();
}

# Archive dimension files
sub redshiftArchiveDimensions() {
    my $self = shift;
    $self->{RS}->archiveDimensions();
}

# Clean archive files
sub redshiftCleanDimensions() {
    my $self = shift;
    $self->{RS}->cleanDimensions();
}




#-----------
# Extract
#-----------
sub extractGetDaily() {
    my $self = shift;
    $self->{EXTRACT}->getDailyFiles();
}

sub extractDiskUsage() {
    my $self = shift;
    $self->{EXTRACT}->getDiskUsage();
}

sub extractValidate() {
    my $self = shift;
    $self->{EXTRACT}->validate();
}

sub extractClean() {
    my $self = shift;
    $self->{EXTRACT}->cleanFiles();
}

sub extractCleanNoFill() {
    my $self = shift;
    $self->{EXTRACT}->cleanNoFillFiles();
}

sub extractGetHourly() {
    my $self = shift;
    $self->{EXTRACT}->getHourlyFiles();
}

#-----------
# Clean
#-----------
sub cleanTmp() {
    my $self = shift;
    $self->{CLEAN}->cleanTmp();
}

sub cleanSubTmp() {
    my $self = shift;
    $self->{CLEAN}->cleanSubTmp();
}

sub cleanUser() {
    my $self = shift;
    $self->{CLEAN}->cleanUser();
}

sub cleanLogs() {
    my $self = shift;
    $self->{CLEAN}->cleanLogs();
}

sub cleanDirs() {
    my $self = shift;
    $self->{CLEAN}->cleanDirs();
}


#-----------
# Test
#-----------
sub test() {
    print "$PROMPT Test\n";
}


1;

