
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

create_hql_file('test', 'US', '2017-01-10','exchange', '12','fill','tll','./test.hql')
