
import os

def run_hive_cmd():
        
    cmd = ["beeline", "-n", "\"jdbc:hive2://ip-172-17-25-136.ec2.internal:2181,ip-172-17-25-137.ec2.internal:2181,ip-172-17-25-135.ec2.internal:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2", "-f", "--hiveconf", "tez.queue.name ==", output_path]

    p = subprocess.popen(cmd)
    
    print 'all complete'
    
