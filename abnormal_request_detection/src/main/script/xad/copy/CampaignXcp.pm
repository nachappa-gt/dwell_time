#
# Copyright (C) 2015.  xAd, Inc.  All rights reserved.
#
=pod

Get campaign hourly summary from S3.

=cut

package xad::copy::CampaignXcp;

use strict;
use warnings;

use xad::Conf;
use xad::DateUtil;
use xad::DirList;
use xad::DirUtil;
use xad::HDFS;
use xad::S3;
use Fcntl qw(:flock);
use IO::LockedFile ( block => 0 );

use base qw(xad::copy::BaseXcp);

my $PROMPT = "[CampaignXcp]";

#-----------------
# Constructor
#-----------------
sub new(@) {
    my $classname = shift;
    die "class method called on object" if ref $classname;

    my $self = xad::copy::BaseXcp->new(@_);
    bless($self, $classname);
    $self->_init();
    return $self;
}


# COMMON
sub _init() {
    my $self = shift;
    die "Missing CFG" unless exists $self->{CFG};
    die "Missing FM" unless exists $self->{FM};
    die "Missing HDFS" unless exists $self->{HDFS};
    die "Missing S3" unless exists $self->{S3};
    die "Missing StatusLog" unless exists $self->{SL};
}



#--------------
# Directories
#-------------

# Get the key used in the status log
sub getStatusLogKey($$$$@) {
    my ($self, $region) = @_;
    my $prefix = $self->{CFG}->get('chs.status_log.prefix');
    my $key = $prefix . "/" . $region;
    return $key;
}

sub getHDFSDir {
    my ($self, @others ) = @_;
    my $base = $self->{CFG}->get('chs.data.prefix.hdfs');
    my $dir = join("/", $base, @others);
    return $dir;
}

sub getS3Dir {
    my ($self, @others) = @_;
    my $base = $self->{CFG}->get('chs.data.prefix.s3');
    my $dir = join("/", $base, @others);
    return $dir;
}


#-----------------
# Start Date Map
#-----------------
sub getStartDateMap() {
    my $self = shift;
    my @countries = $self->getCountries();
    my $retMap = $self->{START_DATE_MAP};

    if (! defined $retMap) {
        print "$PROMPT Start Date Map:\n";
        foreach my $country (@countries) {
            my $date = $self->getCountryStartDate($country);
            next unless defined $date;
            $retMap->{$country} = $date;
            print "$PROMPT - $country => $date\n";
        }
        $self->{START_DATE_MAP} = $retMap;
    }
    return $retMap;
}


#-------------------
# Get Hourly Files
#-------------------

sub getHourly() {
    my $self = shift;
    my $hdfs = $self->{HDFS};

    print "$PROMPT Get Campaign Hourly Summary\n";
    my @dates = $self->getDates('chs.dates');
    my @hours = $self->getHours();
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();

    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Hours     = [@hours]\n";
    print "$PROMPT - Countries = [@countries]\n";

    my $startMap = $self->getStartDateMap();

    foreach my $date (@dates) {
        foreach my $hour (@hours) {
            foreach my $country (@countries) {
                # Check country start date
                my $countryStart = $startMap->{$country};
                if (defined $countryStart && $date lt $countryStart) {
                    print "$PROMPT X SKIP Country $country ($date < $countryStart)\n";
                    next;
                }
                my $logtypes = $logTypeMap->{$country};

                foreach my $logtype (@$logtypes) {
                    my $region = $self->getRegion($country, $logtype);

                    # Check local status 
                    my $localStatusPath = $self->getLocalStatusPath($date, $hour, $region);
                    print "$PROMPT # Processing $date - $hour - $region\n" if $self->{DEBUG};
                    # print "$PROMPT   - local status = $localStatusPath\n" if $self->{DEBUG};
                    if ( -e $localStatusPath) {
                        print "$PROMPT   X [SKIP] found local status.\n" if $self->{DEBUG};
                        next;
                    }

                    # Check HDFS file
                    my $hdfsPath = $self->getHDFSDir($date, $hour, $region);
                    #print "$PROMPT   . HDFS: $hdfsPath\n" if $self->{DEBUG};
                    if ($self->{HDFS}->has($hdfsPath)) {
                        $self->touchLocalStatusLog($localStatusPath, 0);
                        #print "$PROMPT   X [SKIP] found HDFS ($hdfsPath).\n" if $self->{DEBUG};
                        next;
                    }

                    # Check DB Status Log
                    my %sm = $self->getStatusWithMetaData($region, $date, $hour);
                    if (! %sm ||  ! $sm{'status'}) {
                        print "$PROMPT   X [N/A] missing DB Status.\n" if $self->{DEBUG};
                        next;
                    }

                    # Copy
                    my $s3Path = $self->getS3Dir($date, $hour, $region);
                    #my $meta = $sm{'metadata'};
                    #if (defined $meta && $meta > 0) {
                        #print "$PROMPT   . S3: $s3Path (meta=$meta)\n" if $self->{DEBUG};
                        my $ident = "$region $date $hour";
                        $self->{FM}->start($ident, $self->{DEBUG}) and next;   # fork
                        #-------------------------------------------------------------
                        $self->distCopy($s3Path, $hdfsPath);
                        $self->touchLocalStatusLog($localStatusPath);
                        #-------------------------------------------------------------
                        $self->{FM}->finish($ident, $self->{DEBUG});
                    #}
                    #else {
                    #    print "$PROMPT   X SKIP - Invalid Meta Data\n";
                    #    $self->touchLocalStatusLog($localStatusPath);
                    #}
                }
            }
        }
    }
}

sub _getHourly_Helper {
    my $self = shift;
    my ($s3Path, $hdfsPath, $localStatusPath) = @_;
}




#---------
# Status
#---------
sub getStatusWithMetaData {
    my $self = shift;
    my ($region, $date, $hour) = @_;
    my $key = $self->getStatusLogKey($region);
    my %result = $self->{SL}->getStatusWithMetaData($key, "$date/$hour");
    return %result;
}


#
# Get available hours in HDFS.
#
sub getLocalStatusPath {
    my $self = shift;
    my ($date, $hour, $region) = @_;
    my $base = $self->{CFG}->get('chs.log.dir');
    my $path = join("/", $base, $date, $hour, $region);
    return $path;
}

sub getRegion {
    my $self = shift;
    my ($country, $logtype) = @_;
    return $country . "_" . $logtype;
}


#----------------
# Clean Files
#----------------
sub cleanFiles() {
    my $self = shift;
    my $hdfs = $self->{HDFS};

    print "$PROMPT Campaign Hourly Summary - Clean\n";

    my $window = $self->{CFG}->get('chs.keep.window');
    my $keepDate = $self->getBeginDateInKeepWindow($window);

    print "$PROMPT - window = $window\n" if $self->{DEBUG};
    print "$PROMPT - keep date = $keepDate\n" if $self->{DEBUG};

    # Clean HDFS
    my $base = $self->{CFG}->get('chs.data.prefix.hdfs');
    print "$PROMPT + Cleaning HDFS $base ...\n" if $self->{DEBUG};
    $self->deleteBeforeDate($base, $keepDate);

    # Clean Local Status Logs
    my $localBase = $self->{CFG}->get('chs.log.dir');
    print "$PROMPT + Cleaning Local $localBase ...\n" if $self->{DEBUG};
    $self->deleteLocalBeforeDate($localBase, $keepDate);
}



1;

