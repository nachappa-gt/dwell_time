#
# Copyright (C) 2014.  xAd, Inc.  All rights reserved.
#
=pod

=head1 NAME

xad::copy::FetlXcp

=head1 DESCRIPTION

This modules is used to copy Forecast ETL files from 
S3 to HDFS using distcp.

=cut

package xad::copy::FetlXcp;

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

my $PROMPT = "[FetlXcp.pm]";

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

sub _init() {
    my $self = shift;
    die "Missing CFG" unless exists $self->{CFG};
    die "Missing HDFS" unless exists $self->{HDFS};
    die "Missing S3" unless exists $self->{S3};
}


#--------------
# Directories
#--------------
sub getS3Dir($$$@) {
    my ($self, $country, $logtype, $event, @others) = @_;
    return $self->getPrefixedDir('fetl.data.prefix.s3', $country, $logtype, $event, @others);
}

sub getHDFSDir($$$@) {
    my ($self, $country, $logtype, $event, @others) = @_;
    return $self->getPrefixedDir('fetl.data.prefix.hdfs', $country, $logtype, $event, @others);
}


#-----------
# Download
#-----------
# Download files from S3 at daily basis to reduce the number of distcp calls.
sub getDailyFiles() {
    my $self = shift;

    print "$PROMPT Forecast ETL Get Daily\n" if $self->{DEBUG};
    my @dates      = $self->getDates('fetl.dates');
    my @countries  = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    my @events     = split(/\s+/, $self->{CFG}->get('fetl.events'));

    # Debug message
    if ($self->{DEBUG}) {
        print "$PROMPT - Dates     = [@dates]\n";
        print "$PROMPT - Countries = [@countries]\n";
        print "$PROMPT - Events    = [@events]\n";
        foreach my $country (@countries ) {
            my $logtypes = $logTypeMap->{$country};
            print "$PROMPT - Logtypes[$country] = [@$logtypes]\n" if $logtypes;
        }
    }
    my $startMap = $self->getCountryStartDateMap();

    foreach my $date (@dates) {
        foreach my $country (@countries) {
            # Check country start date
            my $countryStart = $startMap->{$country};
            if (defined $countryStart && $date lt $countryStart) {
                print "$PROMPT XX SKIP Country $country ($date < $countryStart)\n";
                next;
            }

            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {
                foreach my $event (@events) {
                    print "$PROMPT Processing $country, $logtype, $event, $date\n" if $self->{DEBUG};

                    # Check destinaton
                    my $dest = $self->getHDFSDir($country, $logtype, $event, $date);
                    print "$PROMPT  - dest=$dest\n" if $self->{DEBUG};
                    if ($self->{HDFS}->has($dest)) {
                        print "$PROMPT  x SKIP $dest (copied)\n";
                        next;
                    }

                    # Validate source
                    my $src  = $self->getS3Dir($country, $logtype, $event, $date);
                    print "$PROMPT  - src=$src\n" if $self->{DEBUG};
                    my $missingHours = xad::DirUtil::getMissingHoursInFolder($src, 1);
                    if (@$missingHours > 0) {
                        print "$PROMPT  x MISSING hours: [@$missingHours]\n";
                        next unless $self->{FORCE};
                    }

                    # Distcp
                    $self->distCopy($src, $dest);
                }
            }
        }
    }
}


#-----------
# Clean
#-----------
sub cleanFiles() {
    my $self = shift;

    print "$PROMPT Forecast ETL Clean\n" if $self->{DEBUG};
    my @countries  = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    my @events     = split(/\s+/, $self->{CFG}->get('fetl.events'));
    my $window = $self->{CFG}->get('fetl.keep.window');
    my $keepDate = $self->getBeginDateInKeepWindow($window);

    # Debug message
    if ($self->{DEBUG}) {
        print "$PROMPT - Keep Date = $keepDate (window = $window)\n";
        print "$PROMPT - Countries = [@countries]\n";
        print "$PROMPT - Events    = [@events]\n";
        foreach my $country (@countries ) {
            my $logtypes = $logTypeMap->{$country};
            print "$PROMPT - Logtypes[$country] = [@$logtypes]\n" if $logtypes;
        }
    }
    foreach my $country (@countries) {
        my $logtypes = $logTypeMap->{$country};
        foreach my $logtype (@$logtypes) {
            foreach my $event (@events) {
                my $base = $self->getHDFSDir($country, $logtype, $event);
                print "$PROMPT Checking: $base\n" if $self->{DEBUG};
                $self->deleteBeforeDate($base, $keepDate);
            }
        }
    }
}



1;
