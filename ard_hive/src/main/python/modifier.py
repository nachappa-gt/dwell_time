from operator import itemgetter
import sys

def read_input(file,separator = '\t'):
    for line in file:
        line.rstrip('\n')              
        yield line.split(separator)

def main(separator='\t'):
   
    data = read_input(sys.stdin, separator = separator)  
   
    try:    
                
        for r in data:
            if r[39] == '1':
                r[39] = ''
            elif r[39] == '0':
                r[39] = '{"abnormal_req":0}'
            elif r[39] == '2':
                r[39] = '{"abnormal_req":1}'
            elif r[39] == '3':
                r[39] = '{"abnormal_req":2}'
            
            print "%s%s%s%s%s" % (r[1],separator,r[39],separator,r[61])
    
   except ValueError:
            
            print "%s%s%s" % (uid,separator,'error')


if __name__ == "__main__":
    main()
