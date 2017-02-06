#
# Copyright (C) 2014.  xAd, Inc.  All rights reserved.
#
=pod

=name

=name

=cut

package xad::copy::EnigmaXcp;

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

my $PROMPT = "[EnigmaXcp]";

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
    my ($self, $country, $logtype, $event) = @_;
    my $prefix = $self->{CFG}->get('enigma.event.folder.prefix');
    return $prefix . $event . "-" . $logtype . "_" . $country;
}

# prefix/country/event/hourly/YYYY/MM/DD
sub getEnigmaDir($$$$@) {
    my ($self, $prefixKey, $country, $logtype, $event, @others) = @_;
    my $eventFolder = $self->getEventFolder($country, $logtype, $event);
    my $hourly = $self->{CFG}->get('enigma.hourly.folder');
    my $dir = $self->getPrefixedDir($prefixKey, $country, $eventFolder, $hourly, @others);
    return $dir;
}

sub getS3Dir($$$$@) {
    my ($self, $country, $logtype, $event, @others) = @_;
    my $defaultKey = 'enigma.data.prefix.s3';
    my $s3PrefixKey = $self->{CFG}->get("$defaultKey.$event", $defaultKey);
    return $self->getEnigmaDir($s3PrefixKey, $country, $logtype, $event, @others);
}

# Get the key used in the status log
sub getStatusLogKey($$$$@) {
    my ($self, $country, $logtype, $event) = @_;
    my $eventFolder = $self->getEventFolder($country, $logtype, $event);
    my $prefix = $self->{CFG}->get('enigma.status_log.prefix');
    my $key = $prefix . $eventFolder;
    return $key;
}

sub getHDFSDir($$$$@) {
    my ($self, $country, $logtype, $event, @others) = @_;
    return $self->getEnigmaDir('enigma.data.prefix.hdfs', $country, $logtype, $event, @others);
}


#-----------
# Validate
#-----------

sub getEventStartDate($$) {
    my ($self, $country, $event) = @_;
    my $baseKey = 'event.start.date';
    my $keys = [
        "$baseKey.$event.$country",
        "$baseKey.$event",
        "$baseKey",
    ];
    my $retval = $self->getFirstKeyMatch($keys);
}

#
# Get a start date map where the keys can be either "country" or "country:event"
sub getCountryEventStartDateMap() {
    my $self = shift;
    my @countries = $self->getCountries();
    my @events = $self->getEventTypes();
    my $retMap = $self->{START_DATE_MAP};

    if (! defined $retMap) {
        foreach my $country (@countries) {
            my $date = $self->getCountryStartDate($country);
            next unless defined $date;
            $retMap->{$country} = $date;

            foreach my $event (@events) {
                $date = $self->getEventStartDate($country, $event);
                next unless defined $date;
                my $key = join(':', $country, $event);
                $retMap->{$key} = $date;
            }
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
    my ($dest, $logtype, $event, $msg) = @_;
    my $retval = 0;

    my $req = ($event eq 'AdRequest' && $logtype ne 'search') ? 1 : 0;
    my $du = ($event eq 'AdUserProfile') ? 1 : 0;
    my $path = $req ?  "$dest/*/*/*/_SUCCESS" : "$dest/*/_SUCCESS";

    if ($du) {
        # Check du, since _SUCCESS is not available
        my %duMap = $self->{HDFS}->du_map($dest);
        my %hourMap = ();
        my $posCount = 0;
        while (my ($path,$size) = each %duMap) {
            if ($path =~ /\d{4}\/\d{2}\/\d{2}\/(\d{2})/) {
                if ($size > 0) {
                    $hourMap{$1} = $size;
                    ++$posCount;
                }
            }
        }
        if ($posCount == 24) {
            $retval = 1;
        }
        elsif (scalar keys %duMap == 0) {
            $retval = -1;
        }
        elsif ($msg) {
            my @hours = xad::DateUtil::expandHours("00:23");
            my @missing = ();
            foreach my $hour (@hours) {
                if (! exists $hourMap{$hour}) {
                    push @missing, $hour;
                }
            }
            $self->{ERR} = "(missing " . join(",", @missing) . ")";
        }
    }
    else {
        # Check _SUCCESS
        my $dirList = xad::DirList::ls_hdfs($path);
        my $size = $dirList->size();
        my $unitCount = $req ? 8 : 1;
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
    }
    return $retval;
}

sub validateHourly {
    my $self = shift;
    my ($dest, $logtype, $event) = @_;
    my $retval = 0;

    my $req = ($event eq 'AdRequest' && $logtype ne 'search') ? 1 : 0;
    my $path = $req ?  "$dest/*/*/_SUCCESS" : "$dest/_SUCCESS";

    my $dirList = xad::DirList::ls_hdfs($path);
    my $size = $dirList->size();
    my $count = $req ? 8 : 1;
    if ( $size == $count) {
        $retval = 1;
    }
    return $retval;
}


# Validate specified HDFS logs
sub validate() {
    my $self = shift;

    print "$PROMPT Validate Engima Logs\n";
    my @dates = $self->getDates('enigma.dates');
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    my @events = $self->getEventTypes();
    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Countries = [@countries]\n";
    print "$PROMPT - Events    = [@events]\n";

    foreach my $date (@dates) {
        print "$PROMPT + CHECKING $date:\n";
        foreach my $country (@countries) {
            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {
                foreach my $event (@events) {
                    my $path = $self->getHDFSDir($country, $logtype, $event, $date);
                    print "$PROMPT   - $path ...";
                    my $status = $self->validateDaily($path, $logtype, $event, 1);
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
}


#----------------
# Enigma Distcp
#----------------
sub getDailyFiles() {
    my $self = shift;
    my $hdfs = $self->{HDFS};

    print "$PROMPT Get Daily Engima Logs\n";
    my @dates = $self->getDates('enigma.dates');
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    my @events = $self->getEventTypes();
    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Countries = [@countries]\n";
    print "$PROMPT - Events    = [@events]\n";

    my $startMap = $self->getCountryEventStartDateMap();

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
                foreach my $event (@events) {

                    # Local status
                    my $localStatusPath = $self->getLocalStatusPath($country, $logtype, $event, $date);
                    if (-e $localStatusPath && ! $self->{FORCE}) {
                        print "$PROMPT x SKIP (found status $localStatusPath)\n";
                        next;
                    }

                    # Check event start date
                    my $key = join(':', $country, $event);
                    my $eventStart = $startMap->{"$country:$event"};
                    if (defined $eventStart && $date lt $eventStart) {
                        print "$PROMPT x SKIP $country:$event (before start date $eventStart\n";
                        next;
                    }

                    my $srcBase = $self->getS3Dir($country, $logtype, $event);
                    my $src = "$srcBase/$date";
                    my $dest = $self->getHDFSDir($country, $logtype, $event, $date);

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
                            if ($self->validateDaily($dest, $logtype, $event) == 1) {
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

                    # Check Enigma status logs (does this work for all event types?)
                    if ($event ne 'AdUserProfile') {
                        my $srcHourMap = $self->getSourceHours($country, $logtype, $event, $date);
                        my @srcHours = sort keys %$srcHourMap;
                        if (@srcHours != 24) {
                            print "$PROMPT x SKIP $country $event $logtype $date " .
                                "(STATUS LOG HOURS = @srcHours)\n";
                            next;
                        }
                    }

                    # Double-check with S3  (optional)
                    my $missingHours = xad::DirUtil::getMissingHoursInDateFolder($srcBase, $date, 0);
                    if (@$missingHours > 0) {
                        print "$PROMPT x SKIP $src (S3 MISSING HOURS = @$missingHours)\n";
                        next unless $self->{FORCE};
                    }

                    print "$PROMPT > Processing: $country - $logtype - $event - $date\n";
                    print "$PROMPT   - src: $src\n";
                    print "$PROMPT   - dest: $dest\n";

                    # Copy
                    $self->distCopy($src, $dest);

                    # Validate results
                    if (! $self->validateDaily($dest, $logtype, $event) == 1) {
                        print "$PROMPT WARNING: VALIDATION FAILED on $dest\n";
                    }
                    $self->touchLocalDateStatusLog($localStatusPath);
                }
            }
        }
    }
}

#----------------
# Enigma DU
#----------------
sub getDiskUsage() {
    my $self = shift;
    my $hdfs = $self->{HDFS};

    print "$PROMPT Enigma Disk Usage\n";
    my @dates = $self->getDates('enigma.dates');
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    my @events = $self->getEventTypes();
    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Countries = [@countries]\n";
    print "$PROMPT - Events    = [@events]\n";

    my $startMap = $self->getCountryEventStartDateMap();

    foreach my $date (@dates) {
        foreach my $country (@countries) {
            # Check country start date
            my $countryStart = $startMap->{$country};
            if (defined $countryStart && $date lt $countryStart) {
                next;
            }

            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {
                foreach my $event (@events) {

                    # Check event start date
                    my $key = join(':', $country, $event);
                    my $eventStart = $startMap->{"$country:$event"};
                    if (defined $eventStart && $date lt $eventStart) {
                        next;
                    }

                    # Check dir
                    my $dir = $self->getHDFSDir($country, $logtype, $event, $date);
                    #($hdfs->has($dir)) || next;

                    # Get du
                    $self->{HDFS}->dus($dir);
                }
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

    print "$PROMPT Enigma Clean\n";
    my @countries = $self->getCountries();
    my @events = $self->getEnigmaEvents();
    my $windowKey = 'enigma.keep.window';
    my $window = $self->getWindow($windowKey);
    my $keepDate = $self->getBeginDateInKeepWindow($window);

    if ($self->{DEBUG}) {
        print "$PROMPT - Countries = [@countries]\n";
        print "$PROMPT - Events    = [@events]\n";
        print "$PROMPT - Window    = $window\n";
        print "$PROMPT - Keep Date = $keepDate\n";
    }

    my $hdfsBase = $self->{CFG}->get('enigma.data.prefix.hdfs');
    foreach my $event (@events) {
        $window = $self->{CFG}->get("$windowKey.$event", $windowKey);
        $keepDate = $self->getBeginDateInKeepWindow($window);
        print "$PROMPT # Clean HDFS: $hdfsBase/*/*_$event-*  ($keepDate, $window days)\n" if $self->{DEBUG};
        foreach my $country (@countries) {
            my $hdfsDir = join("/", $hdfsBase, $country, "*_${event}*", "hourly");
            print "$PROMPT   - Checking: $hdfsDir\n" if $self->{DEBUG};
            $self->deleteBeforeDate($hdfsDir, $keepDate);
        }
    }

    my $localBase = $self->{CFG}->get('enigma.log.dir');
    print "$PROMPT # Clean Local: $localBase\n" if $self->{DEBUG};
    my $eventList = xad::DirList::ls_local($localBase);
    my @names = $eventList->names();
    foreach my $folder (@names) {
        my $dir = "$localBase/$folder";
        $self->deleteLocalBeforeDate($dir, $keepDate);
    }
}


sub cleanNoFillFiles() {
    my $self = shift;
    my $hdfs = $self->{HDFS};

    my @countries = $self->getCountries();
    my @logtypes = ("display", "exchange");
    my @events = ("AdRequest");
    my $window = $self->getWindow('enigma.keep.window.nofill');
    my $keepDate = $self->getBeginDateInKeepWindow($window);

    if ($self->{DEBUG}) {
        print "$PROMPT - Keep Date = $keepDate (window = $window)\n";
        print "$PROMPT - Countries = [@countries]\n";
        print "$PROMPT - Events    = [@events]\n";
        print "$PROMPT - Logtypes  = [@logtypes]\n";
    }

    foreach my $country (@countries) {
        foreach my $logtype (@logtypes) {
            foreach my $event (@events) {
                my $base = $self->getHDFSDir($country, $logtype, $event);
                print "$PROMPT Checking: $base\n" if $self->{DEBUG};
                $self->deleteNF($base, $keepDate);
            }
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

    my $pattern = "$base/*/*/*/*";
    print "$PROMPT - Pattern: $pattern\n" if $self->{DEBUG};
    my $dirList = xad::DirList::ls_hdfs($pattern, "nf", 5);
    my @entries = $dirList->entries();
    my %removeDates = ();
    foreach my $entry (@entries) {
        my $name = $entry->name();
        my ($y,$m,$d,$h,$nf) = split("/", $name);
        my $date = join("/", $y, $m, $d);
        if ($date lt $keepDate) {
            $removeDates{$date}++;
        }
    }
    my @removeList = ();
    foreach my $date (sort keys %removeDates) {
        my $path = join("/", $base, $date, "*", "nf");
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

    print "$PROMPT Get Hourly Enigma Files\n";
    my @dates = $self->getDates('enigma.dates');
    my @hours = $self->getHours();
    my @countries = $self->getCountries();
    my $logTypeMap = $self->getLogTypeMap();
    my @events = $self->getEventTypes();

    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Hours     = [@hours]\n";
    print "$PROMPT - Countries = [@countries]\n";
    print "$PROMPT - Events    = [@events]\n";

    my $startMap = $self->getCountryEventStartDateMap();

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
                foreach my $event (@events) {

                    # Check event start date
                    my $key = join(':', $country, $event);
                    my $eventStart = $startMap->{"$country:$event"};
                    if (defined $eventStart && $date lt $eventStart) {
                        print "$PROMPT x SKIP $country:$event (before start date $eventStart\n";
                        next;
                    }

                    foreach my $hour (@hours) {
                        print "$PROMPT # Checking: $country - $logtype - $event - $date - $hour\n";

                        # Check local status
                        #print "$PROMPT DEBUG - checking local status...\n" if $self->{DEBUG};
                        my $statusPath = $self->getLocalStatusPath($country, $logtype, $event, $date, $hour);
                        if (! $self->{FORCE} && -e $statusPath) {
                            next;
                        }

                        # Check HDFS
                        #print "$PROMPT DEBUG - checking HDFS...\n" if $self->{DEBUG};
                        my $dest = $self->getHDFSDir($country, $logtype, $event, $date, $hour);
                        if ($self->{HDFS}->has($dest)) {
                            if ($self->validateHourly($dest, $logtype, $event) && !$self->{FORCE}) {
                                $self->touchLocalStatusLog($statusPath, 0);
                                next;
                            }
                            $self->{HDFS}->rmrs($dest);
                        }

                        # Check MySQL status log
                        print "$PROMPT DEBUG - checking MySQL Status Log...\n" if $self->{DEBUG};
                        my $status = $self->getDBStatus($country, $logtype, $event, "$date/$hour");
                        if (! $status && ! $self->{FORCE}) {
                            print "$PROMPT x SKIP - Missing MySQL Status.\n" if $self->{DEBUG};
                            last;
                        }

                        my $srcBase = $self->getS3Dir($country, $logtype, $event);
                        my $src = "$srcBase/$date/$hour";

                        print "$PROMPT > Processing: $country - $logtype - $event - $date - $hour (Hourly)\n";
                        print "$PROMPT   - src: $src\n";
                        print "$PROMPT   - dest: $dest\n";

                        $self->distCopy($src, $dest);

                        if (! $self->{NORUN} && ! $self->validateHourly($dest, $logtype, $event)) {
                            print "$PROMPT WARNING - VALIDATION FAILED: $dest\n";
                        }
                        $self->touchLocalStatusLog($statusPath);
                    }
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
    my @events = $self->getEventTypes();
    print "$PROMPT - Dates     = [@dates]\n";
    print "$PROMPT - Countries = [@countries]\n";
    print "$PROMPT - Events    = [@events]\n";

    my $status = 1;
    my $ts = 1;
    foreach my $date (@dates) {
        foreach my $country (@countries) {
            my $logtypes = $logTypeMap->{$country};
            foreach my $logtype (@$logtypes) {
                foreach my $event (@events) {
                    my $key = $self->getStatusLogKey($country, $logtype, $event);
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
}


#---------
# Status
#---------

# 
# Get available hours in S3 using the Status Log.
#
sub getDBStatus {
    my $self = shift;
    my ($country, $logtype, $event, $datehour) = @_;
    my $key = $self->getStatusLogKey($country, $logtype, $event);
    print "$PROMPT DEBUG - status log key = $key / $datehour\n" if $self->{DEBUG};
    my $status = $self->{SL}->getStatus($key, $datehour) || 0;
    return $status;
}


# 
# Get available hours in S3 using the Status Log.
#
sub getSourceHours {
    my $self = shift;
    my ($country, $logtype, $event, $date) = @_;
    my $key = $self->getStatusLogKey($country, $logtype, $event);
    # Return a list of [yyyy/mm/dd/hh]
    my @entries = $self->{SL}->listTime($key, $date);
    my $hourMap = {};
    foreach my $entry (@entries) {
        if ($entry =~ /\d{4}\/\d{2}\/\d{2}\/(\d{2})$/) {
            my $hour = $1;
            $hourMap->{$hour} = 1;
        }
    }
    my @hours = sort keys %$hourMap;
    print "$PROMPT   - Source hours => [" . join(" ", sort keys %$hourMap) . "]\n";
    return $hourMap;
}


#
# Get available hours in HDFS.
#
sub getLocalHDFSHours {
    my $self = shift;
    my ($country, $logtype, $event, $date) = @_;
    my $dir = $self->getLocalStatusPath($country, $logtype, $event, $date);
    my $dirList = xad::DirList::ls_local($dir, '\d\d', 1);
    $dirList->print($dir);
    my $hourMap = { map { $_ => 1} $dirList->names() };
    print "$PROMPT   - HDFS hours   => [" . join(" ", sort keys %$hourMap) . "]\n";
    return $hourMap;
}


sub getLocalStatusPath {
    my $self = shift;
    my ($country, $logtype, $event, @date) = @_;
    my $base = $self->{CFG}->get('enigma.log.dir');
    my $eventFolder = $self->getEventFolder($country, $logtype, $event);
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
    my $base = $self->{CFG}->get('enigma.log.dir');
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

