#
# Copyright (C) 2014.  xAd, Inc.  All rights reserved.
#
=pod

=head1 NAME

xad::copy::XadcmsXcp

=head1 DESCRIPTION

This modules imports xAd Central tables from the
XADCMS database.

=cut

package xad::copy::XadcmsXcp;

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

my $PROMPT = "[XADCMS]";

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
    return $self->getPrefixedDir('xadcms.data.prefix.hdfs', @_);
}

sub getTmpDir(@) {
    my $self = shift;
    my $base = $self->getHdfsUserTmpDir();
    return join("/", $base, @_);
}

sub getURI() {
    my $self = shift;
    my $host = $self->{CFG}->get('xadcms.mysql.host');
    my $port = $self->{CFG}->get('xadcms.mysql.port');
    my $dbname = $self->{CFG}->get('xadcms.mysql.dbname');
    my $uri = "jdbc:mysql://$host:$port/$dbname";
    return $uri;
}

sub getOutputFormat() {
    my $self = shift;
    my $retval = $self->{CFG}->get('xadcms.data.format');
    my %formats = map {$_ => 1} split(/\s+/, $self->{CFG}->get('xadcms.data.formats'));
    if ($self->{FORMAT} && exists $formats{$self->{FORMAT}}) {
        $retval = $self->{FORMAT};
    }
    return $retval;
}


#-----------
# Import
#-----------
# Download XADCMS tables.
sub getTables() {
    my $self = shift;

    # Today's date
    my $dateFormat  = $self->{CFG}->get('xadcms.date.format');
    my $date = xad::DateUtil::convert($dateFormat, xad::DateUtil::today());
    my $outputFormat = $self->getOutputFormat();

    print "$PROMPT XADCMS Get\n" if $self->{DEBUG};
    print "$PROMPT  - date   = $date\n" if $self->{DEBUG};
    print "$PROMPT  - format = $outputFormat\n" if $self->{DEBUG};

    # Connection
    my $uri = $self->getURI();
    my $user = $self->{CFG}->get('xadcms.mysql.user');
    my $passwd = $self->{CFG}->get('xadcms.mysql.passwd');
    my @tables = split(/\s+/, $self->{CFG}->get('xadcms.mysql.tables'));

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
    my $tmpPrefix = $self->{CFG}->get('xadcms.tmp.prefix');
    my $tmp  = $self->getTmpDir($tmpPrefix . $date . "_" . $outputFormat);
    print "$PROMPT  - tmp  = $tmp\n" if $self->{DEBUG};

    # Sqoop
    $self->sqoopImport($uri, $user, $passwd, \@tables, $dest, $tmp, $outputFormat);
}


#-----------
# Clean
#-----------
sub cleanTables() {
    my $self = shift;

    print "$PROMPT XADCMS Clean\n" if $self->{DEBUG};
    my $window  = $self->{CFG}->get('xadcms.keep.window');
    my $dateFmt = $self->{CFG}->get('xadcms.date.format');
    my $keepDate = $self->getBeginDateInKeepWindow($window, -1, $dateFmt);
    my $outputFormat = $self->getOutputFormat();
    print "$PROMPT - Keep Date = $keepDate (window = $window)\n";
    print "$PROMPT - Output format = $outputFormat\n";

    my $base = $self->getHDFSDir($outputFormat);
    $self->deleteHelper($base, 1, $keepDate, $window);
}


1;
