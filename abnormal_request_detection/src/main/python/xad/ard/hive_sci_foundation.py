#!/usr/bin/env python2.7
import sys, os
import time
import datetime
import logging
import logging.config
import logging.handlers as handlers
import getopt, re
from string import Template
import subprocess
import ConfigParser
import shlex
import time
import socket

sys.path.append('/home/xad/sar-optimization/lib/modules/cmnfunc')
import cmnfunc


gl_start_date_str = '2013-12-31'
gl_end_date_str = time.strftime("%Y-%m-%d")
gl_table_names = ['science_core_hrly'];
gl_countries = ['us']
gl_fills =['nf', 'fill']
gl_loc_scores=['tll', 'pos', 'rest']
gl_update_schema=0
gl_cmd_flag = 0
gl_drop_before_add=0
gl_country_prod_type = {'default':'display,exchange,euwest1'}
gl_allowed_table_names = ['science_core_hrly','ex_HttpVendorStats','science_core_orc', 'ex_AdDetails' ,'ex_AdTracking','ex_HttpVendorStats']

algo_logger = logging.getLogger('')
schema_host_type = 'http'

schema_host = "gw01.internal.xad.com" # he2
schema_host = "172.17.19.56" # sci2

raw_data_storage_type='s3'
raw_data_dir_s3 = 's3n://enigma-data-backup/extract'
raw_data_dir_hdfs = '/data/extract'
raw_data_dir_backup_s3 = 's3n://enigma-data/raw-data/camus/data'
gl_raw_data_dir = raw_data_dir_hdfs


def check_source(base_path, country, prod_type,cluster = None):
    ''' Can be used for:
            1. When cluster != None, check the lastest available hdfs data based on xcp log (on other gateway).
               return the lastest finished hdfs hour
            2. When cluster==None, check local log for the last finished hive import hour
    '''
    if cluster!=None:
        cmd = 'ssh ' + cluster + ' ls '+ base_path
    else:
        cmd = 'ls '+ base_path
    cmd = cmd + country + '_' + prod_type + '/'
    lastest_hour = ''

    def lastest(osout):
        ''' input: stdout of 'ls' in current level
            output: The largest number among output numbers '''
        allhours = filter(lambda ih: ih.isdigit(), osout.split('\n')) #only keep numeric output
        return max(allhours)

    for it in range(4): #find the largest year, month, day, hour in log
        out_sys = os.popen(cmd).read()
        last_one = lastest(out_sys)
            return -1
        lastest_hour += last_one
        cmd = cmd + last_one + '/'

    return lastest_hour


def hrly_xcp_data_exist(table_name, curr_hour, prod_type, country):
    ''' Function used to ensure the xcp data for each hour exist. '''
    xcp_log_base_path = gl_log_path['xcp_log']
    cmd = 'test -d ' + xcp_log_base_path + country + '_' + prod_type + '/'
    cmd = cmd + datetime.datetime.strftime(curr_hour, '%Y/%m/%d/%H')
    if os.system(cmd) == 0:
        return True
    elif gl_cmd_flag==1:
        return True
    else:
        algo_logger.error('Input xcp missing data in the middle of the timeframe' + cmd)
        return False


def add_partition_by_hr(  table_name, start_hour_str, end_hour_str, prod_type, country, check_every_hour=True):
    ''' Add data between start_hour and end_hour into table. Works for science_core_hrly 
    '''
    global raw_data_dir_hdfs
    global raw_data_dir_s3
    global raw_data_storage_type

    algo_logger.info('Processing table %s, country: %s, prod_type %s, start %s, end %s' \
        %(table_name, country, prod_type, start_hour_str, end_hour_str))

    local_log_base_path = gl_log_path['local_log']
    table_name = table_name
    prod_type = prod_type

    sql_template = Template("alter table ${table_name} add if not exists partition (cntry='${country}', dt='${dt}', prod_type='${prod_type}', hour='${hour}', fill='${fill}', loc_score='${loc_score}') location '${location}';")
    print prod_type
    print start_hour_str
    print end_hour_str
    
    start_date = datetime.datetime.strptime(start_hour_str, "%Y%m%d%H")
    
    end_date = datetime.datetime.strptime(end_hour_str, "%Y%m%d%H")
    log_str = ''
    cdate = start_date
    log_str0 = 'mkdir -p ' + local_log_base_path + table_name + '/'
    o_str = ""
    o_str += "set tez.queue.name=" + gl_queue + "; \n"
    sqlfile = gl_output_path  +  table_name + datetime.datetime.strftime(datetime.datetime.today(), '%H%M%S')
    fout = open(sqlfile, 'w')
    while( (end_date - cdate) >= datetime.timedelta( hours=0 ) ):
        # check the input data exist for this hour
        if check_every_hour is True:
            if hrly_xcp_data_exist(table_name, cdate, prod_type, country) is False:
                raise Exception('Input xcp missing data in the middle of the timeframe', 'None')

        date_path_str = cdate.strftime("%Y/%m/%d")
        
        raw_data_dir = define_global_dir( table_name, cdate )
        dtstr = datetime.datetime.strftime(cdate, '%Y-%m-%d')
        hour = cdate.hour
        print hour
        if prod_type == 'display_dr':
            fllist = ['nf']
        else:
            fllist = gl_fills
        for fl in fllist: 
            for ls in gl_loc_scores: 
                location = "%s/%s/%s/%s/%02d/%s/%s" % (raw_data_dir, country, prod_type, date_path_str, hour, fl, ls);
                cstr = sql_template.substitute(table_name=table_name, dt=dtstr, hour=hour, country=country, prod_type=prod_type, location=location, fill=fl, loc_score=ls)
                algo_logger.info(cstr)
                o_str = o_str + "\n" + cstr;
run_
        cdate = cdate + datetime.timedelta(hours=1);
    fout.write(o_str)
    fout.close()
    run_hive_cmd_file(sqlfile, post_cmd = log_str, fcmd = o_str)  
    return o_str


def add_partition_by_hr_orc(  table_name, start_hour_str, end_hour_str, prod_type, country  ):
    ''' Add data between start_hour and end_hour into table. Works for science_core_orc
    '''
    #bug: dt and hour need to be string
    global raw_data_dir_hdfs
    global raw_data_dir_s3
    global raw_data_storage_type

    algo_logger.info('Processing table %s, country: %s, prod_type %s, start %s, end %s' \
        %(table_name, country, prod_type, start_hour_str, end_hour_str))

    local_log_base_path = gl_log_path['local_log']
    table_name = table_name
    prod_type = prod_type
    multline_cmd_str = ("insert overwrite table ${table_name} "
                        "partition (cntry, dt, hour, prod_type, fill, loc_score) "
                        "select * from science_core_hrly "
                        "where cntry='${country}' and prod_type='${prod_type}' and (")
    sql_template=Template(multline_cmd_str)
    print prod_type
    print start_hour_str
    print end_hour_str
    
    # Don't run too many partition in one sql; split it into days
    start_date = datetime.datetime.strptime(start_hour_str, "%Y%m%d%H")
    end_date = datetime.datetime.strptime(end_hour_str, "%Y%m%d%H")
    list_time_tuple = []
    while start_date <= end_date:
        next_date = start_date + datetime.timedelta(days =1) - datetime.timedelta(hours=1)
        list_time_tuple.append((start_date, min(next_date, end_date)))
        start_date = next_date + datetime.timedelta(hours=1)

    for it in list_time_tuple:
        sqlfile = gl_output_path +  table_name + datetime.datetime.strftime(datetime.datetime.today(), '%H%M%S')

        istart_date, iend_date = it
        log_str = ''
        cdate = istart_date
        log_str0 = 'mkdir -p ' + local_log_base_path + table_name + '/'
        o_str = ""
        fout = open(sqlfile, 'w')
        o_str += "set tez.queue.name=" + gl_queue + "; \n"
        o_str += "SET hive.exec.dynamic.partition.mode=non-strict; \n" \
                + "SET hive.exec.dynamic.partition=true; \n" \
                + "add jar hdfs:///user/xad/hivejar/jets3t.jar; \n"
        cstr = sql_template.substitute(table_name=table_name, country=country, prod_type=prod_type)
        o_str += cstr
        while( (iend_date - cdate) >= datetime.timedelta( hours=0 ) ):
            
            date_path_str = cdate.strftime("%Y/%m/%d")
            
            raw_data_dir = define_global_dir( table_name, cdate )
            dtstr = datetime.datetime.strftime(cdate, '%Y-%m-%d')
            hour = cdate.hour
            o_str += "(dt='%s' and hour=%d) or " %(dtstr, hour)
            log_str = log_str + log_str0 + "%s_%s/%s/%02d/" % (country, prod_type, date_path_str, hour) + '\n'

            cdate = cdate + datetime.timedelta(hours=1);
        o_str = o_str[0:-4] + ');\n'
        algo_logger.info(o_str)
        fout.write(o_str)
        fout.close()
        run_hive_cmd_file(sqlfile, post_cmd = log_str, queue_size=10, fcmd = o_str)


def partition_catchup_by_hr(table_name, prod_type, country):
    # read cluster information for xcp log 
    xcp_log_cluster = gl_log_path['cluster']
    if xcp_log_cluster =='local':
        xcp_log_cluster = None
    xcp_log_base_path = gl_log_path['xcp_log']
    local_log_base_path = gl_log_path['local_log']

    # Find out the time of lastest finished input and output
    if table_name != 'science_core_orc':
        lastest_input = check_source( xcp_log_base_path, country, prod_type, xcp_log_cluster)
        lastest_input_dt = datetime.datetime.strptime(lastest_input, "%Y%m%d%H")
        lastest_finished = check_source(local_log_base_path+table_name +'/', 
            country, prod_type)
    else:
        lastest_input = check_source(local_log_base_path+'science_core_hrly' +'/', 
            country, prod_type)
        lastest_input_dt = datetime.datetime.strptime(lastest_input, "%Y%m%d%H")
        lastest_finished = check_source(local_log_base_path+table_name +'/', 
            country, prod_type)
        if lastest_input == -1:
            raise Exception('Error getting xcp log', 'Error')
    if lastest_finished == -1: #if previous finish log cant be found, start from 2 day ago
        lastest_finished = datetime.datetime.strftime(lastest_input_dt - datetime.timedelta(days = 2), 
            "%Y%m%d%H")
    else:
        lastest_finished_dt = datetime.datetime.strptime(lastest_finished, "%Y%m%d%H")
        lastest_finished = datetime.datetime.strftime(
            lastest_finished_dt + datetime.timedelta(hours = 1), "%Y%m%d%H")
    if table_name!= 'science_core_orc':
        add_partition_by_hr(table_name, lastest_finished, lastest_input, prod_type, country)
    else: 
        add_partition_by_hr_orc(table_name, lastest_finished, lastest_input, prod_type, country)


def add_partitions(table_name, country, start_dt_str=None, end_dt_str=None, check_every_hour=True):
    ''' If time range is not specified, catchup to lastest input
        else, process specified time range. Need to check input status before processing
    '''
    if( country in gl_country_prod_type ):
        prod_type_str = gl_country_prod_type[country]
    else:
        prod_type_str = gl_country_prod_type['default']

    prod_type_list = prod_type_str.split(',')

    for prod_type in prod_type_list :
        if table_name.startswith('ex_'):
            add_partition_enigma(table_name, start_dt_str, end_dt_str, prod_type, country)
            continue

        # read cluster information for xcp log 
        xcp_log_cluster = gl_log_path['cluster']
        if xcp_log_cluster =='local':
            xcp_log_cluster = None
        xcp_log_base_path = gl_log_path['xcp_log']
        local_log_base_path = gl_log_path['local_log']

        if start_dt_str == None:
            partition_catchup_by_hr(table_name, prod_type, country)

        else:
            start_hour_str = start_dt_str[0:4] + start_dt_str[5:7] + start_dt_str[8:10] + '00'
            # same start/end time will cover a whole day
            end_hour_str = end_dt_str[0:4] + end_dt_str[5:7] + end_dt_str[8:10] + '23' 
            str_processing = 'table: %s, country: %s, prod: %s, start: %s, end %s' \
                %(table_name, country, prod_type, start_dt_str, end_dt_str)

            # check source data status:
            lastest_source = check_source(xcp_log_base_path, country, prod_type, xcp_log_cluster)
            if lastest_source == -1:
                algo_logger.warning('No source data information, skipping execution\n' + str_processing)
                continue
            if lastest_source < start_hour_str:
                algo_logger.warning('Source data not finished in that date frame. skipping\n' \
                    + str_processing)
                continue
            if lastest_source < end_hour_str:
                algo_logger.warning("Source data incomplete yet. Processing to the lastest hour\n" \
                    + str_processing)
                end_hour_str = lastest_source

            if table_name!='science_core_orc':
                add_partition_by_hr(table_name, start_hour_str, end_hour_str, prod_type, country, check_every_hour)
            else:
                add_partition_by_hr_orc(table_name, start_hour_str, end_hour_str, prod_type, country)


def define_global_dir( table_name, cdate=None ):
    global gl_raw_data_dir
    global raw_data_dir_s3
    global raw_data_dir_hdfs
    global raw_data_storage_type
    
    #use retention days from config file
    d_retention = gl_table_retention[table_name]

    if( not cdate ) :
        if( raw_data_storage_type=='s3') :
            gl_raw_data_dir = raw_data_dir_s3
        else:
            gl_raw_data_dir = raw_data_dir_hdfs
    
        if( table_name=='ex_AdUserProfile' or table_name == 'ex_HttpVendorStats') :
            gl_raw_data_dir = raw_data_dir_backup_s3

        return gl_raw_data_dir

    one_day = datetime.timedelta(days=1)
    curr_day = datetime.datetime.now().date();
    cdate = cdate.date()
        
    if( raw_data_storage_type=='s3' ) :
        gl_raw_data_dir = raw_data_dir_s3
    elif ( raw_data_storage_type=='hybrid'):
        if( ( curr_day-cdate) > datetime.timedelta( days=d_retention) ):
            gl_raw_data_dir = raw_data_dir_s3
        else:
            gl_raw_data_dir = raw_data_dir_hdfs
    else:
        gl_raw_data_dir = raw_data_dir_hdfs

    if( table_name=='ex_AdUserProfile' or table_name == 'ex_HttpVendorStats') :
        gl_raw_data_dir = raw_data_dir_backup_s3

    return gl_raw_data_dir

def get_avro_file ( path ):
    shell_cmd = "hadoop fs -ls %s/*.avro" % path
    cmd =["-c", shell_cmd]
    rc, stdout, stderr = cmnfunc.run_cmd_full(cmd)
    if( rc!=0 ):
        return None
    
    sp = stdout.split('\n')
    if len(sp)<=0 :
        return None
    
    row = sp[0]
    file  = row.split(' ')[-1]
    print file
    return file


## update a schema file by extracting it from the avro files, upload the extracted schema file to a http server
def update_schema_http(start_dt_str, table_name):
    
    t_tuple = time.strptime(start_dt_str, "%Y-%m-%d")
    start_date  = datetime.date(*t_tuple[:3])
    date_path_str = start_date.strftime("%Y/%m/%d")
    date_file_str = start_date.strftime("%Y_%m_%d")
    
    define_global_dir( table_name )
    
    # science_core
    cmd_dump_file = '''
        hadoop fs -cat $raw_data_dir/us/display/$date_path_str/00/fill/tll/part-r-00000.avro | head -n100 > $table_name_file
        '''
    shell_cmd = cmd_dump_file.replace("$date_path_str", date_path_str).replace("$table_name", table_name).replace("$raw_data_dir", gl_raw_data_dir)

    # enigma
    if( table_name.startswith('ex_')):
        table_type = table_name.replace("ex_", "")
        prod_type = 'display'
        country = 'us'
        table_id_str = "enigma_event_%s-%s_%s/hourly" % (table_type, prod_type, country)
        
        cmd_dump_file = '''
            hadoop fs -cat $raw_data_dir/us/$table_id_str/$date_path_str/10/part-m-00000.avro | head -n100 > $table_name_file
              '''
        
        shell_cmd = cmd_dump_file.replace("$date_path_str", date_path_str).replace("$table_name", table_name).replace("$raw_data_dir", gl_raw_data_dir).replace("$table_id_str", table_id_str)

        # these two tables are not cleaned
        if( table_name=='ex_AdUserProfile' or table_name == 'ex_HttpVendorStats') :
            path = '$raw_data_dir/us/$table_id_str/$date_path_str/10/'
            full_path = path.replace("$date_path_str", date_path_str).replace("$table_name", table_name).replace("$raw_data_dir", gl_raw_data_dir).replace("$table_id_str", table_id_str)
            file = get_avro_file(full_path)
        
            cmd_dump_file = 'hadoop fs -cat %s | head -n100 > $table_name_file' % file
            shell_cmd = cmd_dump_file.replace("$table_name", table_name)

    cmd =["-c", shell_cmd]
    if( cmnfunc.run_cmd(cmd)==0 ):
        return 0
    
    cmd_extract_schema = '''
        java -jar avro-tools-1.7.6.jar getschema $table_name_file > $dated_schema.avsc
    '''
    
    dated_schema = "%s_%s" % (table_name, date_file_str)
    shell_cmd = cmd_extract_schema.replace("$table_name", table_name).replace("$dated_schema", dated_schema)
    cmd =["-c", shell_cmd]
    if( cmnfunc.run_cmd(cmd)==0 ):
        return 0

    cmd_cp_schema_to_http = '''
        cp $dated_schema.avsc /home/xad/sci_hive_warehouse/schemas/
    '''
    shell_cmd = cmd_cp_schema_to_http.replace("$dated_schema", dated_schema)
    cmd =["-c", shell_cmd]
    if( cmnfunc.run_cmd(cmd)==0 ):
        return 0
    
    return 1

## update a table schema
def update_table(start_dt_str, table_name):
    
    cmd_update_table='''
        alter table $table_name set serdeproperties ('avro.schema.url'='http://$schema_host/hive/$dated_schema.avsc');
    '''
    
    t_tuple = time.strptime(start_dt_str, "%Y-%m-%d")
    start_date  = datetime.date(*t_tuple[:3])
    date_path_str = start_date.strftime("%Y/%m/%d")
    date_file_str = start_date.strftime("%Y_%m_%d")
    
    dated_schema = "%s_%s" % (table_name, date_file_str)
    
    cmd = cmd_update_table.replace("$dated_schema", dated_schema).replace("$table_name", table_name).replace("$schema_host", schema_host)
    return cmnfunc.execute_hive_query(cmd)

def create_table_enigma(start_dt_str, table_name):
    
    cmd_create_table_http= '''
        
        drop table $table_name;
        create external table $table_name
        partitioned by (cntry string, dt string, hour string, prod_type string )
        ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
        with serdeproperties (
        'avro.schema.url'='http://$schema_host/hive/$dated_schema.avsc'
        )
        STORED as INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
        OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
        ;
        '''
    
    t_tuple = time.strptime(start_dt_str, "%Y-%m-%d")
    start_date  = datetime.date(*t_tuple[:3])
    date_path_str = start_date.strftime("%Y/%m/%d")
    date_file_str = start_date.strftime("%Y_%m_%d")
    
    dated_schema = "%s_%s" % (table_name, date_file_str)
    
    cmd = cmd_create_table_http.replace("$dated_schema", dated_schema).replace("$table_name", table_name).replace("$schema_host", schema_host)
    return cmnfunc.execute_hive_query(cmd)


## create a table with given schema
def create_table(start_dt_str, table_name):
    
    if( table_name.startswith('ex_')):
        return create_table_enigma(start_dt_str, table_name)
    
    cmd_create_table_http= '''
        
        drop table $table_name;
        create external table $table_name
        partitioned by (cntry string, dt string, hour string, prod_type string, fill string, loc_score string )
        ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
        with serdeproperties (
        'avro.schema.url'='http://$schema_host/hive/$dated_schema.avsc'
        )
        STORED as INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
        OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
        ;
        '''
    
    t_tuple = time.strptime(start_dt_str, "%Y-%m-%d")
    start_date  = datetime.date(*t_tuple[:3])
    date_path_str = start_date.strftime("%Y/%m/%d")
    date_file_str = start_date.strftime("%Y_%m_%d")
    
    dated_schema = "%s_%s" % (table_name, date_file_str)
    
    cmd = cmd_create_table_http.replace("$dated_schema", dated_schema).replace("$table_name", table_name).replace("$schema_host", schema_host)
    if table_name == 'science_core_orc':
        cmd_orc = "CREATE EXTERNAL TABLE science_core_orc LIKE science_core_hrly STORED AS ORC location \'" + raw_data_dir_orc + "/\';"
        return cmnfunc.execute_hive_query(cmd_orc)
    else:
        return cmnfunc.execute_hive_query(cmd)

##
def run_hive_cmd_file( output_path, post_cmd = None, queue_size=1, fcmd = None):
    global job_queue
    #cmd = ["hive", "-f", output_path]
    cmd = ["beeline", "-n", "xad", "-u",  "\"jdbc:hive2://ip-172-17-25-136.ec2.internal:2181,ip-172-17-25-137.ec2.internal:2181,ip-172-17-25-135.ec2.internal:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\"", "-f", output_path]
    #cmd = ["/usr/hdp/current/hive-server2-hive2/bin/beeline", "-u",  "jdbc:hive2://ip-172-17-25-135.ec2.internal:10500/default", "-f", output_path] 

    algo_logger.info(" ".join(cmd))
    sys.stdout.flush()
    sys.stderr.flush()

    try:
        job_queue
    except NameError:
        job_queue=[]
    while True:
        if len(job_queue) < queue_size:
            #return #not catching the stdout anymore
            p = subprocess.Popen(cmd)
            job_queue.append((p, post_cmd, fcmd) )
            time.sleep(10) # sleep 20s to avoid too frequent hive -f submit. If not sleep, will cause mysterious error
            break
        time.sleep(2)
        for ijob in job_queue:
            if ijob[0].poll() is None:
                continue
            if ijob[0].poll() == 0:
                    os.system(ijob[1])
                job_queue.remove(ijob)
            else:
                job_queue.remove(ijob)
                if ijob[2] is None:
                    returninfo = ijob[1] if ijob[1] is not None else ''
                    raise Exception('Error while executing hive cmd: ', returninfo + '\n' + 'return code: ' + str(ijob[0].poll()))  
                else: #retry if the first attemp failed. Something hive -f filename will send incomplete file.
                    algo_logger.warning('First Attemp failed for job: %s. Return code: %d. Code to retry:\n %s' %(ijob[1], ijob[0].poll(), ijob[2]) )
                    f_retry = '/home/xad/sci_hive_warehouse/tmp/cmd_retry.sql' + str(datetime.datetime.now().minute) + str(datetime.datetime.now().second)
                    with open(f_retry, 'w') as ffcmd:
                        ffcmd.write(ijob[2])
                    run_hive_cmd_file(f_retry, ijob[1], queue_size)



def empty_job_queue():
    ''' function to ensure all the hive jobs are finished'''
    global job_queue
    while len(job_queue) > 0:
        for ijob in job_queue:
            if ijob[0].poll() == None:
                continue
            if ijob[0].poll() == 0:
                if ijob[1] != None:
                    os.system(ijob[1])
                job_queue.remove(ijob)
            else:
                raise Exception('Error while executing hive cmd: ', ijob[1])  

''' # no more output
    ## But do not wait till commad finish, start displaying output immediately ##
    while True:
        out = p.stderr.read(1)
        return_code = p.poll()
        if out == '' and  return_code != None:
            break
        if out != '':
            sys.stdout.write(out)
            sys.stdout.flush()
    # get return code
    return return_code
'''

## add partitions for given dt range, prod_type and country
def add_partition_enigma( table_name, start_dt_str, end_dt_str, prod_type, country ):
    
    global raw_data_dir_hdfs
    global raw_data_dir_s3
    global raw_data_storage_type
    
    sql_template = Template("alter table ${table_name} add if not exists partition (cntry='${country}', dt='${dt}', prod_type='${prod_type}', hour='${hour}') location '${location}';")
    
    if start_dt_str is None:
        start_dt_str = gl_start_date_str
        end_dt_str = gl_end_date_str

    t_tuple = time.strptime(start_dt_str, "%Y-%m-%d")
    start_date  = datetime.date(*t_tuple[:3])
    
    t_tuple = time.strptime(end_dt_str, "%Y-%m-%d")
    end_date  = datetime.date(*t_tuple[:3])
    
    cdate = start_date
    one_day = datetime.timedelta(days=1)
    curr_day = datetime.datetime.now().date();
    while( (end_date - cdate) >= datetime.timedelta( hours=0 ) ):
        sqlfile = gl_output_path +  table_name + datetime.datetime.strftime(datetime.datetime.today(), '%H%M%S')
        date_path_str = cdate.strftime("%Y/%m/%d")
        raw_data_dir = define_global_dir( table_name, datetime.datetime.fromordinal(cdate.toordinal()) )
        
        o_str = ""
        fout = open(sqlfile, 'w');
        
        table_type = table_name.replace("ex_", "")
        table_id_str = "enigma_event_%s-%s_%s/hourly" % (table_type, prod_type, country)

        for hour in range(24) :

            #if hour!= 12 : continue
            location = "%s/%s/%s/%s/%02d" % (raw_data_dir, country, table_id_str, date_path_str, hour);
            cstr = sql_template.substitute(table_name=table_name, dt=str(cdate), hour=hour, country=country, prod_type=prod_type, location=location)
            algo_logger.info(cstr)
            o_str = o_str + "\n" + cstr;
        
        o_str += '\n' #if no new line here, hour 23 will not be processed
        cdate = cdate + one_day;
        fout.write(o_str)
        fout.close()
        run_hive_cmd_file(sqlfile, fcmd = o_str)
    
    return o_str


def drop_partitions_by_day(table_name, curr_dt_str):

    hcmd = "\"alter table %s drop partition (dt='%s')\"" % (table_name, curr_dt_str)
    cmd = ["beeline", "-u",  "jdbc:hive2://", "-e", hcmd]
    algo_logger.info(" ".join(cmd))
    sys.stdout.flush()
    sys.stderr.flush()
    
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=-1)
    
    ## But do not wait till commad finish, start displaying output immediately ##
    while True:
        out = p.stderr.read(1)
        if out == '' and p.poll() != None:
            break
        if out != '':
            sys.stdout.write(out)
            sys.stdout.flush()


def drop_partitions( table_name, start_dt_str, end_dt_str ):
    
    t_tuple = time.strptime(start_dt_str, "%Y-%m-%d")
    start_date  = datetime.date(*t_tuple[:3])
    
    t_tuple = time.strptime(end_dt_str, "%Y-%m-%d")
    end_date  = datetime.date(*t_tuple[:3])
    
    cdate = start_date
    one_day = datetime.timedelta(days=1)
    while( (end_date - cdate) >= datetime.timedelta( hours=0 ) ):
        
        cdate_str = cdate.strftime("%Y-%m-%d")
        drop_partitions_by_day(table_name, cdate_str)
        cdate = cdate + one_day;


def set_cron_param():
    ''' drop before add now only works when date range is specified'''
    global gl_start_date_str
    global gl_end_date_str;
    global gl_output_path;
    global gl_drop_before_add;
    global gl_current_hour_str;

    curr_hour = datetime.datetime.now();
    gl_current_hour_str = datetime.datetime.strftime(curr_hour, '%Y%m%d%H')
    one_day = datetime.timedelta(days=1)
    curr_day = datetime.datetime.now();
    start_dt =  curr_day - one_day;
    end_dt = start_dt;
    gl_start_date_str = start_dt.strftime("%Y-%m-%d")
    gl_end_date_str = gl_start_date_str
    gl_output_path = '/home/xad/sci_hive_warehouse/tmp/add_core_partitions_%s.sql' % gl_current_hour_str
    gl_drop_before_add = 0 # editted drop_before_add to 0 for convenience
    print gl_start_date_str


def parse_cmdline():
    
    global gl_start_date_str
    global gl_end_date_str
    global gl_countries
    global gl_update_schema
    global gl_cmd_flag
    global gl_drop_before_add
    global gl_table_names
    
    # parse command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "-s:e:c:uhndt:", ["startdate=", "enddate=", "countries=", "update_schema", "help", "new_table", "drop_before_add", "tables="] )
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)
    for o, a in opts:
        if o in ("-h", "--help"):
            print "help goes here"
            sys.exit()
        elif o in ("-s", "--startdate"):
            gl_start_date_str = a
            gl_cmd_flag = 1
        elif o in ("-e", "--enddate"):
            gl_end_date_str = a
            gl_cmd_flag = 1
        elif o in ("-c", "--countries"):
            country_str = a
            gl_countries=country_str.split(":")
            # if only countries is given in cmdline, the gl_cmd_flag will not be triggered
        elif o in ("-u", "--update_schema"):
            gl_update_schema = 2
            gl_cmd_flag = 1
        elif o in ("-n", "--new_table"):
            gl_update_schema = 1
            gl_cmd_flag = 1
        elif o in ("-d", "--drop_before_add"):
            # set default drop_before_add to 0; use -d to make it to 1
            gl_drop_before_add = 1
            gl_cmd_flag = 1

        elif o in ("-t", "--tables"):
            table_str = a
            table_names=table_str.split(":")
            for tname in table_names:
                if not ( tname in gl_allowed_table_names ) :
                    print "Table name %s is not recognized." % tname
                    print( "Allowed table names: %s" % ','.join(gl_allowed_table_names) )
                    sys.exit(1)
            gl_table_names = table_names

    RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    if not RE.search(gl_start_date_str):
        print "some error in start_date string: %s" % gl_start_date_str
        sys.exit(1)
    if not RE.search(gl_end_date_str):
        print "some error in end_date string: %s" % gl_end_date_str
        sys.exit(1)

def ConfigSectionMap(Config, section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


def need_drop(table_name, dt):
    ''' Input dt format: %Y-%m%d 
        Assume us_exchange represent the general status
    '''
    if table_name.startswith('ex_'):
        return False
    local_log_base_path = gl_log_path['local_log']
    dt_str = '/'.join(dt.split('-'))
    cmd = 'test -d ' + local_log_base_path + table_name + '/us_display/' + dt_str
    return os.system(cmd)==0


def global_config( config_file = './config/hive_sci_foundation.conf', log_file = './config/logging_sci_foundation.conf' ):
    ''' read params in config file and make them global, also create the logger. In enigma case, some conf does not exist'''
    global gl_start_date_str
    global gl_end_date_str
    global gl_output_path
    global gl_table_names
    global algo_logger
    global gl_cmd_flag
    global algo_logger
    global gl_countries;

    # configure logger
    log_dir = './logs/'
    script_dir = os.path.dirname(os.path.realpath(__file__))
    log_config_file = os.path.normpath(os.path.join(script_dir, log_file))
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    print log_config_file
    logging.config.fileConfig(log_config_file)
    
    log_name = "daily_cron"
    cmnfunc.init_logger(log_name)
    algo_logger = logging.getLogger(log_name)
    algo_logger.handlers[1].doRollover() 

    #add a SMTP handler here # we don't need these lines as the original logger works well
    #host = socket.gethostname()
    #from_addr = 'xad@' + host
    #smtp_handler = handlers.SMTPHandler('localhost', from_addr, 'pan.li@xad.com', 'Hive warehouse error')
    #smtp_handler.setLevel(logging.ERROR)
    #algo_logger.addHandler(smtp_handler)

    #import pdb;
    #pdb.set_trace();
    
    global schema_host
    global schema_host_type
    global raw_data_dir_hdfs
    global raw_data_dir_s3
    global raw_data_dir_backup_s3;
    global gl_raw_data_dir
    global raw_data_dir_orc
    global raw_data_storage_type;
    global gl_country_prod_type
    # path of xcp log, cluster and local finish log path
    global gl_log_path
    global gl_queue
    global gl_table_retention

    config = ConfigParser.ConfigParser()
    config.read(config_file)
    config_schema_map = ConfigSectionMap(config, 'schema')
    
    schema_host = config_schema_map['schema_host']
    schema_host_type = config_schema_map['schema_host_type']
    
    config_storage_map = ConfigSectionMap(config, 'storage')
    raw_data_dir_s3 = config_storage_map['raw_data_dir_s3']
    raw_data_dir_hdfs = config_storage_map['raw_data_dir_hdfs']
    raw_data_storage_type = config_storage_map['raw_data_storage_type']
    raw_data_dir_backup_s3 = config_storage_map['raw_data_dir_backup_s3']

    config_table_map = ConfigSectionMap(config, 'table')
    tablename_list_str = config_table_map['table_list']
    gl_table_names = tablename_list_str.split(",")
    algo_logger.debug(gl_table_names)
    
    country_list_str = config_table_map['countries']
    gl_countries = country_list_str.split(',')
    algo_logger.debug(gl_countries)

    retentions = config_table_map['retention'].split(',')
    gl_table_retention = {}
    for it in range(len(gl_table_names)):
        gl_table_retention[gl_table_names[it]] = int(retentions[it])


    if gl_table_names[0].startswith('ex_'):
        pass
    else:
        raw_data_dir_orc = config_storage_map['raw_data_dir_orc']
        gl_country_prod_type = ConfigSectionMap(config, 'prod-types')
        gl_log_path = ConfigSectionMap(config, 'log-path')
        gl_execution = ConfigSectionMap(config, 'execution')
        gl_queue = gl_execution['queue']


def run_hive_warehouse():
    ''' main function for processing avro file into hive tables'''
    global gl_start_date_str
    global gl_end_date_str # used to avoid unbouned local; they are changed later in global scope

    set_cron_param()
    algo_logger.debug("(1) start: %s, end:%s" % (gl_start_date_str, gl_end_date_str))

    # use cmdline to overwrite cron parameters if any
    parse_cmdline();
    algo_logger.debug("(2) start: %s, end:%s" % (gl_start_date_str, gl_end_date_str))

    # this part is for regular commandline
    if( gl_update_schema>0 ):
        for table_name in gl_table_names:
            algo_logger.info("update schema for table %s" % table_name)
            update_schema_http(gl_start_date_str, table_name)
            if( gl_update_schema==1 ) :
                algo_logger.info("create table %s" % table_name)
                create_table(gl_start_date_str, table_name)
            else:
                algo_logger.info("update table %s" % table_name)
                update_table(gl_start_date_str, table_name)
        
        return
    else:
        algo_logger.info("start: %s, end:%s" % (gl_start_date_str, gl_end_date_str))
        for table_name in gl_table_names : # need to process hrly table before orc. Order in Conf matters.
            # drop only if this flag is set to 1
            if( gl_drop_before_add==1 ):
                drop_partitions( table_name, gl_start_date_str, gl_end_date_str )
            for country in gl_countries :
                if gl_cmd_flag==1 :
                    sql_str = add_partitions( table_name, country, gl_start_date_str, gl_end_date_str)
                else:
                    sql_str = add_partitions( table_name, country)

    # cronjob needs to map the old partition to s3 && drop old orc table and delete data
    # gl_cmd_flag=0 means this job is triggered by cronjob
    if( gl_cmd_flag==0 ):
        # use retention days readed from config file
        for table_name in gl_table_names:
            retention_days = gl_table_retention[table_name]
            curr_day = datetime.datetime.now();

            day2drop = curr_day - datetime.timedelta(days=retention_days + 1)        

            gl_start_date_str = day2drop.strftime("%Y-%m-%d")
            gl_end_date_str = gl_start_date_str
            print (gl_start_date_str,gl_end_date_str)

            # drop the old ones that are mapped to hdfs; need_drop fuction ensure drop only run only per day
            if need_drop(table_name, gl_start_date_str):
                drop_partitions( table_name, gl_start_date_str, gl_end_date_str)
                if table_name != 'science_core_orc':
                    for country in gl_countries :
                        # add the new ones that are mapped to s3 for hrly table
                        sql_str = add_partitions( table_name, country, gl_start_date_str, gl_end_date_str, check_every_hour=False)
                elif table_name =='science_core_orc': # deal with orc table. HDFS data remove is needed
                    cmd = 'hdfs dfs -rm -r ' + raw_data_dir_orc + '/*/dt=' + gl_start_date_str
                    os.system(cmd)
                # remove the log folder of dropped partition
                local_log_base_path = gl_log_path['local_log']
                cmd = 'rm -r ' + local_log_base_path + table_name + '/*/' + '/'.join(gl_start_date_str.split('-'))
                os.system(cmd)

    empty_job_queue() # finish all the hive job



def main( config_file = './config/hive_sci_foundation.conf', log_file = './config/logging_sci_foundation.conf'):
    global_config(config_file, log_file)

    define_global_dir( gl_table_names[0] )
    lockpath = './logs/warehouse_pipeline_locked_for_' +   config_file[9:-5]
    if os.system('test -f ' + lockpath)==0:
        algo_logger.error('Lock exists, previous job running')
        return -1
    else:
        os.system("echo 'Hive warehouse pipeline running' > " + lockpath)
        try:
            run_hive_warehouse()
        except Exception as e:
            algo_logger.error('Error while running hive warehouse pipeline: ' + str(e))
            os.system('rm ' + lockpath)
            raise(e)
        else:
            os.system('rm ' + lockpath)
            os.system('find /home/xad/sci_hive_warehouse/tmp/ -type f -mtime +1 -exec rm -f {} +')



if __name__ == "__main__":
    cmnfunc.env_setup()
    sys.exit(main())


