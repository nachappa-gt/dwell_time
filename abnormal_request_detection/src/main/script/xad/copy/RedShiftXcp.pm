#
# Copyright (C) 2014.  xAd, Inc.  All rights reserved.
#
=pod

=head1 NAME

xad::copy::RedShiftXcp

=head1 DESCRIPTION

This modules is dumps records from RedShift table to S3
and copy the files from S3 to HDFS.

=cut

package xad::copy::RedShiftXcp;

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

my $PROMPT = "[RedShiftXcp.pm]";

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
    die "Missing StatusLog" unless exists $self->{SL};
    die "Missing RedShiftUtil" unless exists $self->{RSU};
}


#---------------------
# Call Tracking Logs
#---------------------
#
# Download CTL records from RedShift to S3
#
sub ctlGetS3() {
    my $self = shift;

    print "$PROMPT Call Tracking Log: RedShift => S3\n";
    my @dates = $self->getDates('ctl.dates');
    my $statusLogKey = $self->{CFG}->get('ctl.status_log.key');
    my $s3Root = $self->{CFG}->get('ctl.data.prefix.s3');
    print "$PROMPT   - Dates = [@dates]\n" if $self->{DEBUG};
    print "$PROMPT   - S3 Root = $s3Root\n" if $self->{DEBUG};
    print "$PROMPT   - Status key = $statusLogKey\n" if $self->{DEBUG};

    foreach my $date (@dates) {
        print "$PROMPT # Checking $date";

        # Check local status
        my $localLog = $self->getLocalStatusLog('ctl.log.dir.s3', $date);
        if (-e $localLog && ! $self->{FORCE}) {
            print " (done)\n";
            next;
        }
        else {
            print "\n";
        }

        # Check status log (in MySQL)
        my $status = $self->{SL}->getStatus($statusLogKey, $date) || "0";
        if (! $status) {
            print "$PROMPT   x SKIP - not ready.\n" if $self->{DEBUG};
            next;
        }

        # Check S3
        my $s3Dir  = $self->ctl_getS3Dir($date);
        my $s3Success = join("/", $s3Dir, $self->{CFG}->get('xcp.file.success'));
        my $hasSuccessFile = $self->{S3}->has($s3Success);
        if ($hasSuccessFile && ! $self->{FORCE}) {
            $self->touchLocalStatusLog($localLog, 0);
            next;
        }

        # Construct SQL
        my $sqlDate = xad::DateUtil::convert('yyyy-MM-dd', $date);
        my $table = $self->{CFG}->get('ctl.table.name');
        my $select = "SELECT * FROM $table WHERE date_trunc(\\'day\\',create_date) = \\'$sqlDate\\'";
        my $toPath = join("/", $s3Dir, $self->{CFG}->get('ctl.file.prefix.s3'));
        my $credentials = $self->getS3Credentials();
        my $sql = "UNLOAD ('$select') ";
        $sql .= "TO '$toPath' ";
        $sql .= "CREDENTIALS '$credentials' ";
        $sql .= "PARALLEL OFF ";
        $sql .= "DELIMITER '\\t' ";
        $sql .= "NULL AS '' ";
        $sql .= "ALLOWOVERWRITE;";

        # Execute the Query
        $self->{RSU}->query($sql);

        # Create the SUCCESS file
        $self->copySuccessToS3($s3Dir);

        # Touch Local Success file
        $self->touchLocalStatusLog($localLog);
    }
}


#
# Copy CTL files from S3 to HDFS
#
sub ctlGetHDFS() {
    my $self = shift;

    print "$PROMPT Call Tracking Log: RedShift => S3\n";
    my @dates = $self->getDates('ctl.dates');
    my $s3Root = $self->{CFG}->get('ctl.data.prefix.s3');
    my $hdfsRoot = $self->{CFG}->get('ctl.data.prefix.hdfs');
    my $statusLogKey = $self->{CFG}->get('ctl.status_log.key');
    print "$PROMPT   - Dates   = [@dates]\n" if $self->{DEBUG};
    print "$PROMPT   - S3 Root = $s3Root\n" if $self->{DEBUG};
    print "$PROMPT   - HDFS    = $hdfsRoot\n" if $self->{DEBUG};

    foreach my $date (@dates) {
        print "$PROMPT # Checking $date";

        # Check local status
        my $localHDFSLog = $self->getLocalStatusLog('ctl.log.dir.hdfs', $date);
        if (-e $localHDFSLog && ! $self->{FORCE}) {
            print " (done)\n";
            next;
        }
        else {
            print "\n";
        }

        # Check Source (S3)
        my $hasS3 = 0;
        my $localS3Log = $self->getLocalStatusLog('ctl.log.dir.s3', $date);
        my $s3Dir  = $self->ctl_getS3Dir($date);
        if (-e $localS3Log) {
            $hasS3 = 1;
        }
        else  {
            my $status = $self->{SL}->getStatus($statusLogKey, $date) || "0";
            if ($status) {
                my $s3Success = join("/", $s3Dir, $self->{CFG}->get('xcp.file.success'));
                $hasS3 = 1 if ($self->{S3}->has($s3Success));
            }
        }
        if (! $hasS3) {
            print "$PROMPT   x SKIP - not ready.\n" if $self->{DEBUG};
            next;
        }

        # Check Destination (HDFS)
        my $hdfsDir =  $self->ctl_getHDFSDir($date);
        if ( $self->{HDFS}->has($hdfsDir) ) {
            if (! $self->{FORCE}) {
                $self->touchLocalStatusLog($localHDFSLog, 0);
                next;
            }
            else {
                $self->{HDFS}->rmdir($hdfsDir);
            }
        }

        # Copy with distcp
        print "$PROMPT   - src  = $s3Dir\n" if $self->{DEBUG};
        print "$PROMPT   - dest = $hdfsDir\n" if $self->{DEBUG};
        $self->distCopy($s3Dir, $hdfsDir);

        # Touch status
        $self->touchLocalStatusLog($localHDFSLog);
    }
}


sub ctl_getS3Dir {
    my $self = shift;
    my $date = shift;
    return join("/", $self->{CFG}->get('ctl.data.prefix.s3'), $date);
}


sub ctl_getHDFSDir {
    my $self = shift;
    my $date = shift;
    return join("/", $self->{CFG}->get('ctl.data.prefix.hdfs'), $date);
}



#----------------
# SUCCESS File
#--------------
sub copySuccessToS3 {
    my $self = shift;
    my $s3Dir = shift;

    # Create local _SUCCESS file
    my $tmpDir = $self->{CFG}->get('proj.tmp.dir');
    my $success = $self->{CFG}->get('xcp.file.success');
    my $src = join("/", $tmpDir, $success);
    my $dest = join("/", $s3Dir, $success);
    if (! -f $src) {
        $self->run("touch $src", 0);
    }

    # Copy to S3
    $self->{S3}->putFile($src, $dest);
}


#--------------
# RedShift
#--------------
sub getS3Credentials {
    my $self = shift;
    my $access_key = $self->{CFG}->get('s3.access_key');
    my $secret_key = $self->{CFG}->get('s3.secret_key');
    my $cred = "aws_access_key_id=$access_key;aws_secret_access_key=$secret_key";
    return $cred;
}


#--------------
# Dimensions
#--------------

sub getDimensions {
    my $self = shift;

    print "$PROMPT Download RedShift Dimension Tables\n";

    my @dwenigma_tables = split(/[,\s]+/,
        $self->{CFG}->get('dwenigma.dimension.tables'));
    my @userstore_tables = split(/[,\s]+/,
        $self->{CFG}->get('userstore.dimension.tables'));
    my @xadcms_tables = split(/[,\s]+/,
        $self->{CFG}->get('xadcms.dimension.tables'));
    my $tmpDir = $self->{CFG}->get('proj.tmp.dir');

    print "$PROMPT  - dwenigma tables = [@dwenigma_tables]\n" if $self->{DEBUG};
    print "$PROMPT  - userstore tables = [@userstore_tables]\n" if $self->{DEBUG};
    print "$PROMPT  - xadcms tables = [@xadcms_tables]\n" if $self->{DEBUG};
    my $hdfsDir = $self->{CFG}->get('redshift.dimension.hdfs.dir');
    my $ext = ".tsv";

    if (! $self->{HDFS}->has($hdfsDir)) {
        $self->{HDFS}->mkdir($hdfsDir);
    }

    # Download rom dwenigma Redshift DB
    my $db = 'dwenigma';
    foreach my $table (@dwenigma_tables) {
        print "$PROMPT # Processing $db.$table ...\n";
        my $output = "$tmpDir/$table$ext";
        my $hdfsPath = "$hdfsDir/$table$ext";
        $self->_downloadRedshiftTable($table, $output, $db);
        $self->_atomicPush($output, $hdfsPath);
    }

    # Download from userstore Redshift DB
    #$db = 'userstore';
    #foreach my $table (@userstore_tables) {
    #    print "$PROMPT # Processing $db.$table ...\n";
    #    my $output = "$tmpDir/$table$ext";
    #    my $hdfsPath = "$hdfsDir/$table$ext";
    #    $self->_downloadRedshiftTable($table, $output, $db);
    #    $self->_atomicPush($output, $hdfsPath);
    #}

    # Download from XADCMS MySQL DB
    $db = 'xadcms';
    foreach my $table (@xadcms_tables) {
        print "$PROMPT # Processing $db.$table ...\n";
        my $output = "$tmpDir/$table$ext";
        my $hdfsPath = "$hdfsDir/$table$ext";
        $self->_downloadMySQLTable($table, $output, $db);
        $self->_atomicPush($output, $hdfsPath);
    }
}


sub _downloadRedshiftTable {
    my $self = shift;
    my $table = shift;
    my $output = shift;
    my $key_prefix = @_ ? shift : 'redshift';

    my $host = $self->{CFG}->get("$key_prefix.conn.host");
    my $port = $self->{CFG}->get("$key_prefix.conn.port");
    my $dbname = $self->{CFG}->get("$key_prefix.conn.dbname");
    my $passwd = $self->{CFG}->get("$key_prefix.conn.password");

    my $cmd .= " PGPASSWORD=". $passwd;
    $cmd .= " psql -h $host -U root -p $port -d $dbname -A -F \$'\\t' -t";
    $cmd .= " -c \"select * from $table\"";
    $cmd .= " -o $output";

    $self->run($cmd);
}

sub _downloadMySQLTable {
    my $self = shift;
    my $table = shift;
    my $output = shift;
    my $key_prefix = @_ ? shift : 'xadcms';

    my $host = $self->{CFG}->get("$key_prefix.mysql.host");
    my $port = $self->{CFG}->get("$key_prefix.mysql.port");
    my $dbname = $self->{CFG}->get("$key_prefix.mysql.dbname");
    my $user = $self->{CFG}->get("$key_prefix.mysql.user");
    my $passwd = $self->{CFG}->get("$key_prefix.mysql.passwd");

    my $cmd .= "mysql -u $user -p$passwd -h $host -P $port $dbname";
    $cmd .= " -e \"SELECT * FROM $table\" --bat --silent";
    $cmd .= " > $output";

    $self->run($cmd);
}


sub _atomicPush {
    my $self = shift;
    my ($src, $dest) = @_;

    my $new  = $dest . "-new";
    my $prev = $dest . "-prev";

    # Put
    if ($self->{HDFS}->has($new)) {
        $self->{HDFS}->rmrs($new);
    }
    if ($self->{HDFS}->has($prev)) {
        $self->{HDFS}->rmrs($prev);
    }
    $self->{HDFS}->put($src, $new);

    # Replace
    if ($self->{HDFS}->has($dest)) {
        $self->{HDFS}->mv($dest, $prev);
    }
    $self->{HDFS}->mv($new, $dest);

    # Clean up, assuming that mv is faster than rm
    if ($self->{HDFS}->has($prev)) {
        $self->{HDFS}->rmrs($prev);
    }
}


#-----------
# Archive
#-----------
# Copy dimension files to the archive folder
sub archiveDimensions {
    my $self = shift;

    print "$PROMPT Archive RedShift Dimension Files\n";

    # Get files in the archive folder
    my $archiveMap = $self->_listArchive();
    my $archiveDir = $self->{CFG}->get('redshift.archive.dir');

    # Prepare tmp dir
    my $tmpDir = $self->getHdfsUserTmpDir();
    if (! $self->{HDFS}->has($tmpDir)) {
        $self->{HDFS}->mkdir($tmpDir);
    }

    # List files in the dimension folder
    my $dimDir = $self->{CFG}->get('redshift.dimension.hdfs.dir');
    my $regex = '\.\w+';
    my $dimList = xad::DirList::ls_hdfs($dimDir, $regex);
    my @entries = $dimList->entries();
    foreach my $entry (@entries) {
        my $name = $entry->name();
        my $ts = $self->_normalizeTimestamp($entry->timestamp());
        my $size = $entry->size();
        print "$PROMPT - $ts $name ($size)\n" if $self->{DEBUG};
        my $key = "$name-$ts";
        if (! exists $archiveMap->{$key}) {
            my $src = "$dimDir/$name";
            my $dest = "$archiveDir/$key";
            my $tmp = "$tmpDir/$key";
            $self->{HDFS}->cp($src, $tmp);
            $self->{HDFS}->mv($tmp, $dest);
        }
    }
}


# List and parse files in the archive folder.
# Return a map of entry-yyyy-mm-dd-hhmm, e.g.,
#
#   adgroup_dimension.tsv-2016-03-30-0120
#   footprints_brands_dimension.tsv-2016-03-28-0120
#
sub _listArchive {
    my $self = shift;
    my $dir = $self->{CFG}->get('redshift.archive.dir');
    if (! $self->{HDFS}->has($dir)) {
        $self->{HDFS}->mkdir($dir);
    }
    my $list = xad::DirList::ls_hdfs($dir);
    my $map = { map {$_ => 1} $list->names() };
    return $map;
}


# Convert HDFS timestamp to archive timestamp
sub _normalizeTimestamp {
    my $self = shift;
    my $ts = shift;
    my $retval = "NA";
    if ($ts =~ /(\d{4})-(\d{2})-(\d{2}):(\d{2}):(\d{2})/) {
        $retval = sprintf("%04d-%02d-%02d-%02d%02d", $1, $2, $3, $4, $5);
    }
    else {
        die "Invalid timestamp '$ts'";
    }
}

#-----------
# Clean
#-----------

sub cleanDimensions {
    my $self = shift;

    print "$PROMPT Clean RedShift Dimension Archive\n";

    my $dir = $self->{CFG}->get('redshift.archive.dir');
    my $window = $self->{CFG}->get('redshift.archive.keep.window');
    print "$PROMPT  - archive dir = $dir\n" if $self->{DEBUG};
    print "$PROMPT  - window = $window\n" if $self->{DEBUG};

    # List and organize into a map: orig_name => [archive_name array]
    my $list = xad::DirList::ls_hdfs($dir);
    my @names = $list->names();
    my %map = ();
    foreach my $archive_name (@names) {
        if ($archive_name =~/(.+)-\d{4}-\d{2}-\d{2}-\d{4}/) {
            my $orig_name = $1;
            push @{$map{$orig_name}}, $archive_name;
        }
        else {
            print "$PROMPT WARNING - invalid name $archive_name\n";
        }
    }

    # Print
    my @rmList = ();
    while (my ($k,$v) = each %map ) {
        print "$PROMPT + $k:\n" if $self->{DEBUG};
        my $size = scalar (@$v);
        foreach my $idx (0..($size-1)) {
            my $keep = ($size - $idx <= $window) ? 1 : 0;
            my $name = $v->[$idx];
            if ($keep) {
                print "$PROMPT   - $name (keep)\n" if $self->{DEBUG};
            }
            else {
                print "$PROMPT   X $name (DELETE)\n" if $self->{DEBUG};
                my $path = "$dir/$name";
                push @rmList, $path;
            }
        }
    }

    # Remove files
    foreach my $path (@rmList) {
        $self->{HDFS}->rmrs($path);
    }
}


1;

