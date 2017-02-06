#
# Copyright (C) 2016.  xAd, Inc.  All rights reserved.
#
=pod

=head1 NAME

xad::copy::POIXcp

=head1 DESCRIPTION

This modules imports xAd Central tables from the
POI database.

=cut

package xad::copy::PoidbXcp;

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

my $PROMPT = "[POIDB]";

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
sub getHDFSDir(@) {
    my $self = shift;
    return $self->getPrefixedDir('poidb.data.prefix.hdfs', @_);
}

sub getTmpDir(@) {
    my $self = shift;
    my $base = $self->getHdfsUserTmpDir();
    return join("/", $base, @_);
}

sub getURI() {
    my $self = shift;
    my $host = $self->{CFG}->get('poidb.conn.host');
    my $port = $self->{CFG}->get('poidb.conn.port');
    my $dbname = $self->{CFG}->get('poidb.conn.dbname');
    my $uri = "jdbc:postgresql://$host:$port/$dbname";
    return $uri;
}

sub getOutputFormat() {
    my $self = shift;
    my $retval = $self->{CFG}->get('poidb.data.format');
    return $retval;
}


#-----------
# Import
#-----------
# Download POI tables.
sub getTables() {
    my $self = shift;

    # Today's date
    my $date = $self->getDate();
    my $outputFormat = $self->getOutputFormat();

    print "$PROMPT POI DB - Get\n" if $self->{DEBUG};
    print "$PROMPT  - date   = $date\n" if $self->{DEBUG};
    print "$PROMPT  - format = $outputFormat\n" if $self->{DEBUG};

    # Connection
    my $uri = $self->getURI();
    my $user = $self->{CFG}->get('poidb.conn.user');
    my $passwd = $self->{CFG}->get('poidb.conn.passwd');
    my @tables = split(/\s+/, $self->{CFG}->get('poidb.tables'));

    print "$PROMPT  - uri    = $uri\n" if $self->{DEBUG};
    print "$PROMPT  - user   = $user\n" if $self->{DEBUG};
    print "$PROMPT  - tables = [@tables]\n" if $self->{DEBUG};

    # Check destinaton
    my $dest = $self->getHDFSDir($outputFormat, $date);
    print "$PROMPT  - dest = $dest\n" if $self->{DEBUG};

    if ($self->{HDFS}->has($dest) && ! $self->{FORCE}) {
        print "$PROMPT  x SKIP - Found $dest\n";
        return;
    }

    # Tmp dir
    my $tmpPrefix = $self->{CFG}->get('poidb.tmp.prefix');
    my $tmpDir  = $self->getTmpDir($tmpPrefix);
    my $tmp  = join("_", $tmpDir, $outputFormat, $date);
    print "$PROMPT  - tmp  = $tmp\n" if $self->{DEBUG};

    # Sqoop
    $self->sqoopImport($uri, $user, $passwd, \@tables, $dest, $tmp, $outputFormat);
}


#---------------
# Split Tables
#---------------
sub splitTables() {
    my $self = shift;

    print "$PROMPT POI DB - Split\n";

    # Get information ...
    my $srcFmt = $self->{CFG}->get('poidb.data.format');
    my $splitFmt = $self->{CFG}->get('poidb.data.format.split');
    my $date = $self->getDate();
    my $srcDir = $self->getHDFSDir($srcFmt, $date);
    my $destDir = $self->getHDFSDir($splitFmt, $date);
    my $tmpPrefix = $self->{CFG}->get('poidb.tmp.prefix');
    my $tmpDir  = $self->getTmpDir($tmpPrefix);

    print "$PROMPT  - date = $date\n" if $self->{DEBUG};
    print "$PROMPT  - source dir = $srcDir\n" if $self->{DEBUG};
    print "$PROMPT  - dest dir = $destDir\n" if $self->{DEBUG};

    my @tables = split(/\s+/, $self->{CFG}->get('poidb.tables.split'));
    foreach my $table (@tables) {
    print "$PROMPT # Processing table $table ...\n" if $self->{DEBUG};
        my $src = "$srcDir/$table";
        my $dest = "$destDir/$table";
        my $tmp  = join("_", $tmpDir, $splitFmt, $date, $table);
        print "$PROMPT  - source = $src\n" if $self->{DEBUG};
        print "$PROMPT  - dest = $dest\n" if $self->{DEBUG};
        print "$PROMPT  - tmp = $tmp\n" if $self->{DEBUG};

        # Call pyspark to split

        # Move tmp to destination
    }
}



#-----------
# Clean
#-----------
sub cleanTables() {
    my $self = shift;

    print "$PROMPT POI DB - Clean\n" if $self->{DEBUG};
    my $window  = $self->{CFG}->get('xadcms.keep.window');
    my $dateFmt = $self->{CFG}->get('xadcms.date.format');
    my $keepDate = $self->getBeginDateInKeepWindow($window, -1, $dateFmt);
    my $outputFormat = $self->getOutputFormat();
    print "$PROMPT - Keep Date = $keepDate (window = $window)\n";
    print "$PROMPT - Output format = $outputFormat\n";

    my $base = $self->getHDFSDir($outputFormat);
    $self->deleteHelper($base, 1, $keepDate, $window);
}


#------------------
# Helper Functions
#------------------
sub getDate() {
    my $self = shift;
    my $dateFormat  = $self->{CFG}->get('poidb.date.format');
    my $dateStr = $self->{DATE} ? $self->{DATE} : xad::DateUtil::today();
    my $date = xad::DateUtil::convert($dateFormat, $dateStr);
    return $date;
}


1;
