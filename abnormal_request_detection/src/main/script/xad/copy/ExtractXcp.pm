#
# Copyright (C) 2014.  xAd, Inc.  All rights reserved.
#
=pod

=name

=cut

package xad::copy::ExtractXcp;

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

my $PROMPT = "[ExtractXcp]";

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
    die "Missing S3" unless exists $self->{S3};
    die "Missing StatusLog" unless exists $self->{SL};
}


#--------------
# Directories
#-------------

sub getEventFolder($$$) {
    my ($self, $country, $logtype) = @_;
    return  $country . "_" . $logtype;
}

# prefix/country/event/hourly/YYYY/MM/DD
sub getEnigmaDir($$$@) {
    my ($self, $prefixKey, $country, $logtype, @others) = @_;
    my $dir = $self->getPrefixedDir($prefixKey, $country, $logtype, @others);
    #print "$PROMPT - Enigma dir is $dir\n";
    return $dir;
}

sub getS3Dir($$$$@) {
    my ($self, $country, $logtype, @others) = @_;
    my $s3PrefixKey = 'extract.data.prefix.s3';
    return $self->getEnigmaDir($s3PrefixKey, $country, $logtype, @others);
}

# Get the key used in the status log
sub getStatusLogKey($$$@) {
    my ($self, $country, $logtype) = @_;
    my $eventFolder = $self->getEventFolder($country, $logtype);
    my $prefix = $self->{CFG}->get('extract.status_log.prefix');
    my $key = $prefix . "/" . $eventFolder;
    return $key;
}

sub getHDFSDir($$$@) {
    my ($self, $country, $logtype, @others) = @_;
    return $self->getEnigmaDir('extract.data.prefix.hdfs', $country, $logtype, @others);
}


#-----------
# Validate
#-----------

sub getEventStartDate($$) {
    my ($self, $country, $event) = @_;
    my $baseKey = 'event.start.date';
    my $keys = [
        "$baseKey.$country",
        "$baseKey",
    ];
    my $retval = $self->getFirstKeyMatch($keys);
}

#
# Get a start date map where the keys can be a "country"
sub getCountryStartDateMap() {
    my $self = shift;
    my @countries = $self->getCountries();
    my $retMap = $self->{START_DATE_MAP};

    if (! defined $retMap) {
        foreach my $country (@countries) {
            my $date = $self->getCountryStartDate($country);
            $retMap->{$country} = $date;

        }
        $self->{START_DATE_MAP} = $retMap;
    }
    return $retMap;
}


# Return:
#   1 : valid
#   0 : empty or incomplete
#  -1 : path not found.
#
# If msg is 1, return missing hours in $self->{ERR}
#
sub validateDaily {
    my $self = shift;
    my ($dest, $logtype, $msg) = @_;
    my $retval = 0;

    my $path = "$dest/*/*/*/_SUCCESS";

        # Check _SUCCESS
        my $dirList = xad::DirList::ls_hdfs($path);
        my $size = $dirList->size();
        my $unitCount = 6 ;
        my $totalCount = $unitCount * 24;
        if ( $size == $totalCount ) {
            $retval = 1;
        }
        elsif ($msg) {
            # Identify missing hours
            my %map = ();
            my @entries = $dirList->entries();
            foreach my $entry (@entries) {
                my $path = $entry->path();
                if ($path =~ /\d{4}\/\d{2}\/\d{2}\/(\d{2})/) {
                    my $hour = $1;
                    $map{$hour}++;
                }
            }
            my @hours = xad::DateUtil::expandHours("00:23");
            my @missing = ();
            foreach my $hour (@hours) {
                my $count = $map{$hour};
                if (! $count || $count < $unitCount) {
                    push @missing, $hour;
                }
            }
            $self->{ERR} = "(missing " . join(",", @missing) . ")";
        }
    return $retval;
}

sub validateHourly {
    my $self = shift;
    my ($dest) = @_;
    my $retval = 0;

    my $path =  "$dest/*/*/_SUCCESS";

    my $dirList = xad::DirList::ls_hdfs($path);
    my $size = $dirList->size();
    my $count = 6;
    if ( $size == $count) {
        $retval = 1;
    }
    return $retval;
}


# Validate specified HDFS logs
sub validate() {
    my $self = shift;

    print "$PROMPT Validate Extract Logs\n";
    my @dates = $self->getDates('enigma.dates');
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
#    my @events = $self->getEventTypes();
    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Countries = [@countries]\n";
 #   print "$PROMPT - Events    = [@events]\n";

    foreach my $date (@dates) {
        print "$PROMPT + CHECKING $date:\n";
        foreach my $country (@countries) {
            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {
                my $path = $self->getHDFSDir($country, $logtype, $date);
                    print "$PROMPT   - $path ...";
                    my $status = $self->validateDaily($path, $logtype, 1);
                    if ($status == 1) {
                        print " pass\n";    # valiad
                    }
                    elsif ($status == 0) {
                        print " FAILED " . $self->{ERR} . "\n";  # empty/incomplete
                    }
                    else {
                        print " N/A\n";     # not available
                    }
            }
        }
    }
}


#----------------
# Enigma Distcp
#----------------
sub getDailyFiles() {
    my $self = shift;
    my $hdfs = $self->{HDFS};

    print "$PROMPT Get Daily Engima Extracts\n";
    my @dates = $self->getDates('enigma.dates');
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Countries = [@countries]\n";

    my $startMap = $self->getCountryStartDateMap();

    foreach my $date (@dates) {
        foreach my $country (@countries) {
            # Check country start date
            my $countryStart = $startMap->{$country};
            if (defined $countryStart && $date lt $countryStart) {
                print "$PROMPT X SKIP Country $country ($date < $countryStart)\n";
                next;
            }

            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {
                    # Local status
                    my $localStatusPath = $self->getLocalStatusPath($country, $logtype, $date);
                    if (-e $localStatusPath && ! $self->{FORCE}) {
                        print "$PROMPT x SKIP (found status $localStatusPath)\n";
                        next;
                    }

                    my $src = $self->getS3Dir($country, $logtype, $date);
                    my $dest = $self->getHDFSDir($country, $logtype, $date);

                    # Check destinaton
                    my $dirList = xad::DirList::ls_hdfs($dest);
                    my @hours = $dirList->names();
                    if (@hours) {
                        if ($self->{FORCE}) {
                            $self->{HDFS}->rmrs($dest);
                            $self->run("rm -rf $localStatusPath");
                        }
                        elsif (@hours == 24) {
                            # Double check that they are valid
                            if ($self->validateDaily($dest, $logtype) == 1) {
                                $self->touchLocalDateStatusLog($localStatusPath, 0);
                                print "$PROMPT x SKIP $dest (copied)\n";
                                next;
                            }
                            else {
                                $self->{HDFS}->rmrs($dest);
                            }
                        }
                        else {
                            print "$PROMPT WARNING: HDFS $dest (INCOMPLETE: @hours)\n";
                            $self->{HDFS}->rmrs($dest);
                        }
                    }

                    # Double-check with S3  (optional)
           #         my $missingHours = xad::DirUtil::getMissingHoursInDateFolder($srcBase, $date, 0);
           #         if (@$missingHours > 0) {
           #             print "$PROMPT x SKIP $src (S3 MISSING HOURS = @$missingHours)\n";
           #             next unless $self->{FORCE};
           #         }

                    print "$PROMPT > Processing: $country - $logtype - $date\n";
                    print "$PROMPT   - src: $src\n";
                    print "$PROMPT   - dest: $dest\n";

                    # Copy
                    $self->distCopy($src, $dest);

                    # Validate results
                    if (! $self->validateDaily($dest, $logtype) == 1) {
                        print "$PROMPT WARNING: VALIDATION FAILED on $dest\n";
                    }
                    $self->touchLocalDateStatusLog($localStatusPath);
            }
        }
    }
}

#----------------
# Extract DU
#----------------
sub getDiskUsage() {
    my $self = shift;
    my $hdfs = $self->{HDFS};

    print "$PROMPT Eextract Disk Usage\n";
    my @dates = $self->getDates('enigma.dates');
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Countries = [@countries]\n";

    my $startMap = $self->getCountryStartDateMap();

    foreach my $date (@dates) {
        foreach my $country (@countries) {
            # Check country start date
            my $countryStart = $startMap->{$country};
            if (defined $countryStart && $date lt $countryStart) {
                next;
            }

            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {
                # Check dir
                my $dir = $self->getHDFSDir($country, $logtype, $date);
                # Get du
                $self->{HDFS}->dus($dir);
            }
        }
    }
}

#----------------
# Clean Files
#----------------
sub cleanFiles() {
    my $self = shift;
    my $hdfs = $self->{HDFS};
    print "$PROMPT Extract Clean\n";
    my @countries = $self->getCountries();

    my $windowKey = 'extract.keep.window';
    my $window = $self->getWindow($windowKey);
    my $keepDate = $self->getBeginDateInKeepWindow($window);

    if ($self->{DEBUG}) {
        print "$PROMPT - Window    = $window\n";
        print "$PROMPT - Keep Date = $keepDate\n";
    }

    my $hdfsBase = $self->{CFG}->get('extract.data.prefix.hdfs');

    foreach my $country(@countries) {
        my @logtypes = $self->getCountryLogTypes($country);
        foreach my $logtype(@logtypes) {
            my $dir = join('/', $hdfsBase, $country, $logtype);
            print "$PROMPT   - Checking: $dir\n" if $self->{DEBUG};
            $self->deleteBeforeDate($dir, $keepDate);
        }
    }

    my $localBase = $self->{CFG}->get('extract.log.dir');
    print "$PROMPT # Clean Local: $localBase\n" if $self->{DEBUG};
    my $list = xad::DirList::ls_local($localBase);
    my @names = $list->names();
    foreach my $folder (@names) {
        my $dir = "$localBase/$folder";
        $self->deleteLocalBeforeDate($dir, $keepDate);
    }
}


sub cleanNoFillFiles() {
    my $self = shift;
    my $hdfs = $self->{HDFS};
    my @countries = $self->getCountries();

    my $window = $self->getWindow('extract.keep.window.nofill');
    my $keepDate = $self->getBeginDateInKeepWindow($window);

    if ($self->{DEBUG}) {
        print "$PROMPT - Window    = $window\n";
        print "$PROMPT - Keep Date = $keepDate (window = $window)\n";
    }
    my $hdfsBase = $self->{CFG}->get('extract.data.prefix.hdfs');
    print "$PROMPT Checking: $hdfsBase\n" if $self->{DEBUG};
    foreach my $country(@countries) {
        my @logtypes = $self->getCountryLogTypes($country);
        foreach my $logtype(@logtypes) {
            my $dir = join('/', $hdfsBase, $country, $logtype);
            print "$PROMPT   - Checking: $dir\n" if $self->{DEBUG};
            $self->deleteNF($dir, $keepDate);
        }
    }
}


#
# Delete "nf" folders.
#
sub deleteNF($$) {
    my $self = shift;
    my $base = shift;
    my $keepDate = shift;

    my $pattern = "$base/*/*/*";
    print "$PROMPT - Pattern: $pattern\n" if $self->{DEBUG};
    my $dirList = xad::DirList::ls_hdfs($pattern, "nf", 5);
    my @entries = $dirList->entries();
    my %removeDates = ();
    foreach my $entry (@entries) {
        my $name = $entry->name();
        my ($y,$m,$d,$h,$l,$nf) = split("/", $name);
        my $date = join("/", $y, $m, $d);
        if ($date lt $keepDate) {
            $removeDates{$date}++;
        }
    }
    my @removeList = ();
    foreach my $date (sort keys %removeDates) {
        my $path = join("/", $base, $date, "*", "*", "nf");
        push @removeList, $path;
    }

    if (@removeList) {
        $self->{HDFS}->rmrs(join(" ", @removeList));
    }
}



#-------------------
# Get Hourly Files
#-------------------

sub getHourlyFiles() {
    my $self = shift;
    my $hdfs = $self->{HDFS};

    print "$PROMPT Get Hourly Extract Files\n";
    my @dates = $self->getDates('enigma.dates');
    my @hours = $self->getHours();
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();

    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Hours     = [@hours]\n";
    print "$PROMPT - Countries = [@countries]\n";

    my $startMap = $self->getCountryStartDateMap();

    foreach my $date (@dates) {
        foreach my $country (@countries) {
            # Check country start date
            my $countryStart = $startMap->{$country};
            if (defined $countryStart && $date lt $countryStart) {
                print "$PROMPT X SKIP Country $country ($date < $countryStart)\n";
                next;
            }

            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {

                # Check local daily status
                my $localDailyStatus = $self->getLocalStatusPath($country, $logtype, $date, '_DONE');
                if (! $self->{FORCE} && -e $localDailyStatus) {
                    next;
                }

                my $numHours = 0;
                foreach my $hour (@hours) {
                    print "$PROMPT # Checking : $country - $logtype - $date - $hour\n";

                    # Check hourly local status
                    my $localStatusPath = $self->getLocalStatusPath($country, $logtype, $date, $hour);
                    if (! $self->{FORCE} && -e $localStatusPath) {
                        ++$numHours;
                        next;
                    }

                    # Check MySQL status; apply offset for data feeds
                    my $statusDate = $self->{OFFSET} ? 
                        xad::DateUtil::addDeltaDays($date, $self->{OFFSET}) : $date;
                    my $status = $self->getDBStatus($country, $logtype, "$statusDate/$hour");
                    if (! $status) {
                        last;
                    }

                    # Check HDFS
                    my $dest = $self->getHDFSDir($country, $logtype, $date, $hour);
                    if (!$self->{FORCE} && $self->{HDFS}->has("$dest/nf/tll/_SUCCESS")) {
                        $self->touchLocalStatusLog($localStatusPath, 0);
                        ++$numHours;
                        next;
                    }

                    my $src= $self->getS3Dir($country, $logtype, $date, $hour);
                    print "$PROMPT > Processing: $country - $logtype - $date - $hour (Hourly)\n";
                    print "$PROMPT   - src: $src\n";
                    print "$PROMPT   - dest: $dest\n";

                    $self->distCopy($src, $dest);

                    $self->touchLocalStatusLog($localStatusPath);
                    ++$numHours;
                }

                if ($numHours == 24) {
                    $self->touchLocalStatusFile($localDailyStatus, 0);
                }
            }
        }
    }
}

#-------------
# Status Log
#------------

sub displayStatusLog {
    my $self = shift;
    print "$PROMPT Display Status Logs\n";

    my @dates = $self->getDates('enigma.dates');
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Countries = [@countries]\n";

    my $status = 1;
    my $ts = 1;
    foreach my $date (@dates) {
        foreach my $country (@countries) {
            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {
                    my $key = $self->getStatusLogKey($country, $logtype);
                    # Get a list of [timestamp  yyyy/mm/dd/hh]
                    my @entries = $self->{SL}->listTime($key, $date, $status, $ts);
                    print "$PROMPT + $key\n";
                    foreach my $entry (@entries) {
                        print "$PROMPT   - $entry\n";
                    }
            }
        }
    }
}


#---------
# Status
#---------

# 
# Get available hours in S3 using the Status Log.
#
sub getDBStatus {
    my $self = shift;
    my ($country, $logtype, $datehour) = @_;
    my $key = $self->getStatusLogKey($country, $logtype);
    my $status = $self->{SL}->getStatus($key, $datehour);
    return $status;
}


sub getLocalStatusPath {
    my $self = shift;
    my ($country, $logtype, @date) = @_;
    my $base = $self->{CFG}->get('extract.log.dir');
    my $eventFolder = $self->getEventFolder($country, $logtype);
    my $path = join("/", $base, $eventFolder, @date);
    return $path;
}

#---------
# List
#---------

# List the last local status log
sub listLocal {
    my $self = shift;

    print "$PROMPT Last Local Status\n";
    my $base = $self->{CFG}->get('extract.log.dir');
    my $today = xad::DateUtil::today('yyyy/MM/dd');

    # Get event names
    my $dirList = xad::DirList::ls_local($base);
    my @events = $dirList->names();

    # Get last entries
    my %map = ();
    foreach my $event (@events) {
        my $dir = "$base/$event";
        my $time = $self->getLastLocalTime($dir);
        # Check if there is delay
        my $delay = "";
        if ($time =~ /(\d{4}\/\d{2}\/\d{2}):/) {
            my $date = $1;
            my $diff = xad::DateUtil::deltaDays($today, $date);
            if ($diff > 1) {
                if ($diff > 3) {
                    $delay = " (***)";
                }
                elsif ($diff > 2) {
                    $delay = " (**)";
                }
                else {
                    $delay = " (*)";
                }
            }
        }
        else {
            $delay = " (ERROR)";
        }
        my $line = sprintf(" - %-65s %s%s\n", $dir,$time, $delay);

        my @items = split(/[-_]/, $event);
        my $country = $items[-1];
        my $logtype = $items[-2];
        my $key = "$country:$logtype";
        push @{$map{$key}}, $line;
    }
    print "\n# Results\n";
    my @keys = sort keys %map;
    foreach my $key (@keys) {
        print "$key:\n";
        my $lines = $map{$key};
        print @$lines;
    }
}

sub getLastLocalTime {
    my $self = shift;
    my $dir = shift;
    my $depth = @_ ? shift : 4;

    my @entries = ();
    my $output = "";
    foreach my $i (1..4) {
        my $entry = $self->getLastEntry($dir);
        last unless defined $entry;
        push @entries, $entry;
        $dir .= "/$entry";
        if ($i > 1) {
            $output .= ($i > 3) ? ":" : "/";
        }
        $output .= $entry;
    }
    return $output;
}

sub getLastEntry {
    my $self = shift;
    my $dir = shift;
    my $entry = undef;
    if (-e $dir) {
        my $dirList = xad::DirList::ls_local($dir);
        my @names = $dirList->names();
        if (@names) {
            $entry = $names[-1];
        }
    }
    return $entry;
}

1;

