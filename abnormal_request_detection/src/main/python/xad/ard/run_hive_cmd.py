import subprocess

def run_hive_cmd(country,logtype,date,hour,fill,sl):
        
    ##logging.info("Running Hive Command Line......")
         
    queue = "xianglingmeng"
    table_name = "xianglingmeng.science_core_orc"
    output_path = "hive.hql"
    create_hql_file(table_name, country, date, logtype, hour, fill,sl, output_path)
    cmd = ["beeline", "-n", "\"jdbc:hive2://ip-172-17-25-136.ec2.internal:2181,ip-172-17-25-137.ec2.internal:2181,ip-172-17-25-135.ec2.internal:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2","--hiveconf", "tez.queue.name ==", queue, "-f", output_path]

    command = ' '.join(cmd)
        
    p = subprocess.Popen(command)



## This script is to test to build a hql file with required queries
## It will generate the hql file even it doesn't exist before

from string import Template


def create_hql_file(table_name, country, dt, prod_type, hour, fill, loc_score, hql_path):


    query_template = Template("create table ${table_name} if not exists partition (cntry='${country}', dt='${dt}', prod_type='${prod_type}', hour='${hour}', fill='${fill}', loc_score='${loc_score}');")
    
    queue = "xianglingmeng";

    hive_cmd = ""
    hive_cmd += 'set tez.queue.name = ' + queue + ";\n"
    
    hql_file = open(hql_path, 'w')
    ## we can make changes in the for loop to generate queies for different country, dt, prod_typ, hour, fill and loc_score
    for i in range(2):
        query = query_template.substitute(table_name = table_name, country = country, dt = dt, prod_type = prod_type, hour = hour, fill= fill, loc_score = loc_score)
    	hive_cmd += query + "\n"     
    
    hql_file.write(hive_cmd)
    hql_file.close()

run_hive_cmd('us', 'exchange','2017-01-10', '12', 'fill', 'tll')


