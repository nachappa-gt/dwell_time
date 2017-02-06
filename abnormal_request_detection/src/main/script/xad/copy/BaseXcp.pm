#
# Copyright (C) 2014.  xAd, Inc.  All rights reserved.
#
=pod

=name

=name

=cut

package xad::copy::BaseXcp;

use strict;
use warnings;

use xad::Conf;
use xad::DateUtil;
use xad::DirList;
use xad::DirUtil;
use xad::ForkManager;
use xad::HDFS;
use xad::S3;

use File::Basename;
use Fcntl qw(:flock);
use IO::LockedFile ( block => 0 );

use base qw(xad::OptionContainer);

my $PROMPT = "[BaseXcp]";

#-----------------
# Constructor
#-----------------
sub new(@) {
    my $classname = shift;
    die "class method called on object" if ref $classname;

    my $self = xad::OptionContainer->new(@_);
    bless($self, $classname);
    $self->_init();
    return $self;
}

sub _init() {
    my $self = shift;
}


#---------
# Exec
#---------
sub run  {
    my $self = shift;
    my $cmd = shift;
    my $norun = @_ ? shift : $self->{NORUN};
    print "$PROMPT RUN> $cmd\n";
    unless ($norun) {
        !system($cmd) || die "$PROMPT ERROR running '$cmd': $!\n";
    }
}


#-----------
# Options
#-----------
sub getConfigArray($$) {
    my $self = shift;
    my $confKey = shift;
    my $optKey = shift;

    my $valStr = (exists $self->{$optKey}) ? $self->{$optKey} :
        $self->{CFG}->get($confKey);
    my @vals = split(/[,\s]+/, $valStr);
    return @vals;
}

sub getCountries(@) {
    my $self = shift;
    my $key = @_ ? shift : 'default.countries';
    return $self->getConfigArray($key, 'COUNTRY');
}

sub getEventTypes(@) {
    my $self = shift;
    my $key = @_ ? shift : 'enigma.events';
    return $self->getConfigArray($key, 'EVENT');
}

sub getDates(@) {
    my $self = shift;
    my $key = @_ ? shift : 'default.dates';
    my $fmt = $self->{CFG}->get('default.date.format');
    my $dateStr = $self->{DATE} ? $self->{DATE} : 
        $self->{CFG}->get($key, $key);
    my @dates =  xad::DateUtil::convert($fmt, xad::DateUtil::expand($dateStr));
    return @dates;
}

sub getHours(@) {
    my $self = shift;
    my $hourStr = shift;
    if (! defined $hourStr) {
        $hourStr = (defined $self->{HOUR}) ? $self->{HOUR} : "00-23";
    }
    my @hours = xad::DateUtil::expandHours($hourStr);
    return @hours;
}

sub getLogTypes(@) {
    my $self = shift;
    my $key = @_ ? shift : 'default.logtypes';
    return $self->getConfigArray($key, 'LOGTYPE');
}

sub getCountryLogTypes($) {
    my $self = shift;
    my $country = shift;
    my $key = 'default.logtypes';
    if ($self->{CFG}->has("$key.$country")) {
        $key = "$key.$country";
    }
    return $self->getConfigArray($key, 'LOGTYPE');
}

# Create a "county => [logtypes]" map.
sub getLogTypeMap($) {
    my $self = shift;
    my $retMap = $self->{LOGTYPE_MAP};
    if (!defined $retMap) {
        $retMap = {};
        my @countries = $self->getCountries();
        foreach my $country (@countries) {
            my $logtypes = [$self->getCountryLogTypes($country)];
            $retMap->{$country} = $logtypes;
        }
        $self->{LOGTYPE_MAP} = $retMap;
    }
    return $retMap;
}


sub getEnigmaEvents {
    my $self = shift;
    my $key = @_ ? shift : 'enigma.events';
    my $str = $self->{CFG}->get($key);
    my @events = split(/[\s,]+/, $str);
    return @events;
}


sub getWindow {
    my $self = shift;
    my $key = shift;
    my $window = $self->{WINDOW} ? $self->{WINDOW} :
        $self->{CFG}->get($key);
    return $window;
}


#-------------------
# Log Start Dates
#-------------------

# Get a "country => start date" map.
sub getCountryStartDateMap() {
    my $self = shift;
    my @countries = $self->getCountries();
    my $retMap = {};

    foreach my $country (@countries) {
        my $date = $self->getCountryStartDate($country);
        next unless defined $date;
        $retMap->{$country} = $date;
    }
    return $retMap;
}

sub getCountryStartDate($) {
    my ($self, $country) = @_;
    my $baseKey = 'country.start.date';
    my $keys = [
        "$baseKey.$country",
        "$baseKey",
    ];
    return $self->getFirstKeyMatch($keys);
}

sub getFirstKeyMatch($) {
    my ($self, $keys) = @_;
    my $retval;
    foreach my $key (@$keys) {
        if ($self->{CFG}->has($key)) {
            $retval = $self->{CFG}->get($key);
            last;
        }
    }
    return $retval;
}



#-------------------
# Directory
#-------------------
sub getParentDir($) {
    my $self = shift;
    my $path = shift;
    my ($filename, $dir) = fileparse($path);
    return $dir;
}

sub getCountryLogtypeEventDir($$$$@) {
    my ($self, $prefixKey, $country, $logtype, $event, @others) = @_;
    my $prefix = $self->{CFG}->get($prefixKey, $prefixKey);
    my $dir = join("/", $prefix, $country, $logtype, $event);
    if (@others) {
        $dir = join("/", $dir, @others);
    }
    return $dir;
}

sub getPrefixedDir($@) {
    my ($self, $prefixKey, @entries) = @_;
    my $prefix = $self->{CFG}->get($prefixKey, $prefixKey);
    my $dir = join("/", $prefix, @entries);
    return $dir;
}


sub getHdfsUserTmpDir() {
    my $self = shift;
    return join("-", $self->{CFG}->get('hdfs.tmp.dir'), $ENV{'USER'});
}


#-------------------
# Distcp Related
#-------------------
sub distCopy_retry($$@) {
    my $self = shift;
    my $src = shift;
    my $dest = shift;
    my $maxTries = @_ ? shift : 3;
    my $sleep = @_ ? shift : 5;
}


sub distCopy($$@) {
    my ($self, $src, $dest, $tmp) = @_;
    my $hdfs = $self->{HDFS};

    # Make tmp dir
    $tmp = $self->getHdfsUserTmpDir() unless $tmp;
    unless ($hdfs->has($tmp)) {
        $hdfs->mkdir_hdfs($tmp);
    }

    # Make desination parent dir
    my $parentDir  = $self->getParentDir($dest);
    unless ($hdfs->has($parentDir)) {
        $hdfs->mkdir_hdfs($parentDir);
    }

    # Make an atomic copy
    #$self->_runDistCpAtomic($src,$dest,$tmp);
    $self->_runDistCpAtomicViaTmp($src,$dest,$tmp);
}


# Run distcp with the -atomic option
sub _runDistCpAtomic($$@) {
    my $self = shift;
    my ($src, $dest, $tmp) = @_;
    my $cmd = $self->makeDistcpGetAtomicCmd($src, $dest, $tmp);
    $self->run($cmd);
}

# Generate the -atomic effect with a tmp folder
sub _runDistCpAtomicViaTmp($$@) {
    my $self = shift;
    my ($src, $dest, $tmp) = @_;
    if (!$tmp) {
        $tmp = $self->getHdfsUserTmpDir();
    }
    my ($cmd,$tmpDest) = $self->_makeDistcpGetTmpAtomicCmd($src, $dest, $tmp);

    # Create the tmp folder for update
    if ($self->{HDFS}->has($tmpDest)) {
        $self->{HDFS}->rmrs($tmpDest) unless $self->{KEEP};
    }
    $self->{HDFS}->mkdir($tmpDest);

    # Copy to the tmp folder
    $self->run($cmd);

    # Double check the destination does not exist
    if ($self->{HDFS}->has($dest)) {
        $self->{HDFS}->rmrs($dest);
    }

    # Move to the tmp folder to the target location
    $self->{HDFS}->mv($tmpDest, $dest);
}


# Create the distcp command.
sub _makeDistcpGetAtomicCmd($$@) {
    my $self = shift;
    my ($src, $dest, $tmp) = @_;

    my $maxmaps = $self->{MAXMAPS} ?  $self->{MAXMAPS} :
        $self->{CFG}->get('distcp.max.maps');

    # Change s3 to s3n
    my $hadoop_s3 = $self->{CFG}->get('hadoop.s3');
    $src =~ s/s3:/${hadoop_s3}:/;

    my $cmd = "hadoop distcp";
    $cmd .= " -Dmapred.job.queue.name=" . $self->{QUEUE} if $self->{QUEUE};
    $cmd .= " -Dtez.queue.name=" . $self->{QUEUE} if $self->{QUEUE};
    $cmd .= " -atomic";
    $cmd .= " -m $maxmaps" if ($maxmaps > 0);
    $cmd .= " -tmp $tmp" if ($tmp);
    $cmd .= " $src $dest";
    return $cmd;
}


# Create the distcp command.
sub _makeDistcpGetTmpAtomicCmd($$@) {
    my $self = shift;
    my ($src, $dest, $tmp) = @_;

    my $maxmaps = $self->{MAXMAPS} ?  $self->{MAXMAPS} :
        $self->{CFG}->get('distcp.max.maps');

    # Change s3 to s3n
    my $hadoop_s3 = $self->{CFG}->get('hadoop.s3');
    $src =~ s/s3:/${hadoop_s3}:/;
    my $key = $dest;
    $key =~ s/\///;  # remove leading /
    $key =~ s/\//-/g;  # replace / with -
    my $tmpDest = "$tmp/WIP-$key";
    my $cmd = "hadoop distcp";
    $cmd .= " -Dmapred.job.queue.name=" . $self->{QUEUE} if $self->{QUEUE};
    $cmd .= " -Dtez.queue.name=" . $self->{QUEUE} if $self->{QUEUE};
    $cmd .= " -update";
    $cmd .= " -m $maxmaps" if ($maxmaps > 0);
    $cmd .= " $src $tmpDest";
    return ($cmd, $tmpDest);
}



#-------------------
# Sqoop Related
#-------------------

sub sqoopImport($$$$$$) {
    my ($self, $uri, $user, $passwd, $tables, $dest, $tmp, $outputFormat) = @_;

    # Prepare tmp dir
    $self->{HDFS}->rmr($tmp) if ($self->{HDFS}->has($tmp));
    $self->{HDFS}->mkdir($tmp);

    # Set driver
    my $driver = 'com.mysql.jdbc.Driver';
    if ($uri =~ /postgresql/) {
        $driver = 'org.postgresql.Driver';
    }

    # Get dbname
    my $dbname = "*";
    if ($uri =~ /\/(\w+)$/) {
        $dbname = $1;
    }

    # Improt one table at a time
    foreach my $table (@$tables) {
        my $key = join(".", 'sqoop.import.option', $dbname, $table);
        my $tableOpt = $self->{CFG}->has($key) ? $self->{CFG}->get($key) : undef;
        print "$PROMPT # Downloading $table ...\n" if $self->{DEBUG};
        print "$PROMPT   - table option = $tableOpt\n" if $self->{DEBUG};
        $self->sqoopImportTable($uri, $driver, $user, $passwd, $table,
                                $tmp, $outputFormat, $tableOpt);

        # Validate that the _SUCCESS file exists.
        # Sqoop may not throw exception on failure.
        my $successPath = "$tmp/$table/_SUCCESS";
        if (!$self->{NORUN} && ! $self->{HDFS}->has($successPath)) {
            die "Failed importing $table";
        }
    }

    # Double check that all tables are downloaded.
    unless ($self->{NORUN}) {
        my $list = xad::DirList::ls_hdfs($tmp);
        if ($list->size() < scalar @$tables) {
            my @missing = ();
            foreach my $table (@$tables) {
                push @missing, $table unless ($list->hasName($table));
            }
            die "Missing tables (@missing)";
        }
    }

    # Move tmp to dest
    my $destParent  = $self->getParentDir($dest);
    $self->{HDFS}->mkdir_hdfs($destParent) unless $self->{HDFS}->has($destParent);
    $self->{HDFS}->rmr($dest) if $self->{HDFS}->has($dest);
    $self->{HDFS}->mv($tmp, $dest);
}


# Import a single table
sub sqoopImportTable() {
    my $self = shift;
    my ($uri, $driver, $user, $passwd, $table, $dest, $outputFormat, $tableOpt) = @_;

    my $cmd = "sqoop import";
    $cmd .= " -D mapreduce.job.user.classpath.first=true";
    $cmd .= " -D mapred.child.java.opts=\" -Duser.timezone=GMT\"";
    $cmd .= " -D mapred.job.queue.name=" . $self->{QUEUE} if $self->{QUEUE};
    $cmd .= " -D tez.queue.name=" . $self->{QUEUE} if $self->{QUEUE};
    $cmd .= " --connect $uri" .
        '?dontTrackOpenResources=true\&defaultFetchSize=10000\&useCursorFetch=true';
    $cmd .= "  --driver $driver";
    $cmd .= " --username $user --password $passwd";
    $cmd .= " --table $table";
    $cmd .= " -m " . $self->{MAXMAPS} if ($self->{MAXMAPS} and ! $tableOpt =~/-m\s/);
    $cmd .= " " . $tableOpt if $tableOpt;
    $cmd .= " --warehouse-dir $dest";
    if ($outputFormat && $outputFormat eq "avro") {
        $cmd .= " --as-avrodatafile";
    }
    $self->run($cmd);
}


#-------------------
# Delete
#-------------------
#
# Delete folders before the specified date.
# It will first examine HDFS at the date level. 
# Then, it will check if there are empty month/year folders that
# need to be deleted.
#
sub deleteBeforeDate {
    my $self = shift;
    my $base = shift;
    my $keepDate = shift;
    my $deleteEmptyMonthYear = @_ ? shift : 1;

    my ($y,$m,$d) = split("/", $keepDate);

    # Delete days
    my $delDays = $self->deleteHelper("$base/*/*", 3, $keepDate);
    if ($delDays > 0 && $deleteEmptyMonthYear) {
        # Delete empty months
        my $delMonths = $self->deleteHelper("$base/*", 2, "$y/$m");
        if ($delMonths > 0) {
            # Delete empty years
            $self->deleteHelper($base, 1, $y);
        }
    }
}


# A helper function to delete at date, month or year level.
sub deleteHelper($$$@) {
    my $self = shift;
    my $path = shift;
    my $nameLevel = shift;
    my $dateThreshold = shift;
    my $minSize = @_ ? shift : 3;   # min number of files to keep

    my $dirList = xad::DirList::ls_hdfs($path, "", $nameLevel);
    my @entries = $dirList->entries();

    my @removeList = ();

    if (scalar @entries > $minSize) {
        foreach my $entry (@entries) {
            my $name = $entry->name();
            if ($name lt $dateThreshold) {
                my $path = $entry->path();
                push @removeList, $path;
            }
        }
        if (@removeList > 0) {
            $self->{HDFS}->rmrs(join(" ", @removeList));
        }
    }
    return scalar @removeList;
}


# Delete local folders before specified date
sub deleteLocalBeforeDate {
    my $self = shift;
    my ($dir, $keepDate) = @_;

    xad::DirUtil::deleteYMD($dir, $keepDate, $self->{NORUN});
}


#
# Get the first day of the keep window.  It optionally takes
# a date offset (default is +1).  So keep window 1 is will be expanded
# as "L1+1", which is yesterday.
#
sub getBeginDateInKeepWindow($@) {
    my $self = shift;
    my $window = shift;
    my $offset = @_ ? shift : "-1";
    my $fmt = @_ ? shift : $self->{CFG}->get('default.date.format');

    # Check offset
    if ($offset =~ /^\d+$/) {
        $offset = "+" . $offset;
    }

    # Use "LN+m" to describe the keep window.
    my $dateStr = "L$window" . $offset;

    # Use decode to convert the date string into a date range.
    my @dates = sort {$a cmp $b} split(":", xad::DateUtil::decode($dateStr));
    die "Failed to expand $dateStr" unless (@dates);

    # Convert to the desired date format
    my $first = xad::DateUtil::convert($fmt, $dates[0]);
    return $first;
}


#------------
# Local Log
#------------
sub getLocalStatusLog {
    my $self = shift;
    my ($key, $date) = @_;
    return join("/", $self->{CFG}->get($key), $date);
}

# Touch a local status dir
sub touchLocalStatusLog {
    my $self = shift;
    my $log = shift;
    my $norun = @_ ? shift : $self->{NORUN};
    $self->run("mkdir -p $log", $norun);
}

# Touch a local status file
sub touchLocalStatusFile {
    my $self = shift;
    my $path = shift;
    my $norun = @_ ? shift : $self->{NORUN};
    my $parent = dirname($path);
    $self->run("mkdir -p $parent", $norun);
    $self->run("touch $path", $norun);
}

sub touchLocalDateStatusLog {
    my $self = shift;
    my $datePath = shift;
    my $norun = @_ ? shift : $self->{NORUN};

    if (! -e $datePath) {
        $self->run("mkdir -p $datePath", $norun);
    }
    my @hours = xad::DateUtil::expandHours("00-23");
    foreach my $hour (@hours) {
        my $hourPath = "$datePath/$hour";
        if (! -e $hourPath) {
            $self->run("mkdir -p $hourPath", $norun);
        }
    }
}


#
# Check the date status.  It considered complete if
# 1. The date path exists, AND
# 2A. there are no sub-folders, OR
# 2B. it has 24 hourly sub-folders.
#
sub checkLocalDateStatus {
    my $self = shift;
    my $datePath = shift;
    my $retval = 0;
    if (-e $datePath) {
        my $list = xad::DirList::ls_local($datePath);
        my @names = $list->names();
        my $numHours = scalar @names;
        if ($numHours == 0) {
            $retval = 1;
        }
        elsif ($numHours == 24) { 
            $retval = 2;
        }
    }
    return $retval;
}


1;


