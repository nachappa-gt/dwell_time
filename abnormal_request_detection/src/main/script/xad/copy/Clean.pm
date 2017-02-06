#
# Copyright (C) 2014.  xAd, Inc.  All rights reserved.
#
=pod

=head1 NAME

xad::copy::Clean.pm -- General purpose clean module.

=cut

package xad::copy::Clean;

use strict;
use warnings;

use xad::Conf;
use xad::DateUtil;
use xad::DirList;
use xad::DirUtil;
use xad::HDFS;

use base qw(xad::copy::BaseXcp);

my $PROMPT = "[Clean.pm]";

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
    die "Missing HDFS" unless exists $self->{HDFS};
}


#--------------------
# Clean Tmp Folders
#--------------------
sub cleanTmp() {
    my $self = shift;

    print "$PROMPT Clean HDFS Tmp Folders\n";
    my $tmpDir = $self->{CFG}->get('clean.hdfs.tmp.dir');
    my $threshold = $self->_getCleanThresholdDate();
    $self->_cleanDir($tmpDir, $threshold, 'temp');
    $self->_cleanDir($tmpDir, $threshold, '\w+-\w+-\w+-');
}


sub cleanSubTmp() {
    my $self = shift;
    my @patterns = split(/\s+/, $self->{CFG}->get('clean.hdfs.sub.tmp.patterns'));
    print "$PROMPT Clean HDFS Sub-Tmp Folders (@patterns)\n";
    my $threshold = $self->_getCleanThresholdDate();
    foreach my $regex (@patterns) {
        my $tmpDir = $self->{CFG}->get('clean.hdfs.tmp.dir');
        my $dirList = xad::DirList::ls_hdfs($tmpDir, $regex);
        my @hiveFolders = $dirList->names();

        foreach my $folder (@hiveFolders) {
            my $dir = "$tmpDir/$folder";
            $self->_cleanDir($dir, $threshold);
        }
    }
}


#--------------------
# Clean User Folders
#--------------------
sub cleanUser() {
    my $self = shift;
    my @folders = split(/\s+/, $self->{CFG}->get('clean.hdfs.user.tmp.folders'));
    print "$PROMPT Clean HDFS User Folders (@folders)\n";
    my $threshold = $self->_getCleanThresholdDate();
    foreach my $folder (@folders) {
        my $dir = "/user/*/$folder";
        $self->_cleanDir($dir, $threshold);
    }
}


#--------------------
# Clean App Logs
#--------------------
sub cleanLogs() {
    my $self = shift;
    print "$PROMPT Clean App Logs\n";
    my $baseDir = "/app-logs";
    my $dirList = xad::DirList::sudo_ls_hdfs("hdfs", $baseDir);
    my @names = $dirList->names();
    my $threshold = $self->_getCleanThresholdDate();
    foreach my $name (@names) {
        my $dir = join("/", $baseDir, $name, "logs");
        $self->_cleanDir($dir, $threshold);
    }
}


#-----------------------
# Clean Specified Dirs
#-----------------------
sub cleanDirs() {
    my $self = shift;
    die "Use --dir to specify desired directories" unless ($self->{DIR});
    my $dirStr = $self->{DIR};
    my @dirs = split(/,/, $dirStr);
    my $threshold = $self->_getCleanThresholdDate();
    print "$PROMPT Clean @dirs (threshold = $threshold)\n";

    foreach my $dir (@dirs) {
        $self->_cleanDir($dir, $threshold)
    }
}



#--------------------
# Utils
#--------------------

#
# Get the cleaning threshold date, derived from the keep window.
# Use --window to override.
#
sub _getCleanThresholdDate() {
    my $self = shift;
    my $window = $self->{WINDOW} ? $self->{WINDOW} :
        $self->{CFG}->get('clean.hdfs.tmp.keep.window');
    my $today = xad::DateUtil::today('yyyy-MM-dd');
    my $threshold = xad::DateUtil::addDeltaDays($today, -$window);
    return $threshold;
}

#
# Clean the specified directory.
#
sub _cleanDir {
    my $self = shift;
    my ($dir, $threshold, $regex) = @_;

    print "$PROMPT ## Cleaning $dir (threshold=$threshold) ...\n";
    my $dirList = xad::DirList::sudo_ls_hdfs("hdfs", $dir, $regex);
    my @entries = $dirList->entries();
    my @rmList = ();
    foreach my $entry (@entries) {
        my $date = $entry->date();   
        my $path = $entry->path();   
        if ($date le $threshold) {
            print "$PROMPT  - $date $path (DELETE)\n" if $self->{DEBUG};
            push @rmList, $path;
        }
        else {
            print "$PROMPT  + $date $path (KEEP)\n" if $self->{DEBUG};
        }
    }
    if (@rmList > 0) {
        my @tmpList = ();
        my $count = 0;
        my $maxList = 10;
        while (@rmList) {
            push @tmpList, shift @rmList;
            if (++$count >= $maxList) {
                $self->{HDFS}->sudo_rmrs("hdfs", "@tmpList");
                @tmpList = ();
                my $count = 0;
            }
        }
        if (@tmpList) {
            $self->{HDFS}->sudo_rmrs("hdfs", "@tmpList");
        }
    }
}


1;
