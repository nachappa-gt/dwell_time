import sys
def read_input(file,separator = '\t'):
    for line in file:
        # split the line into words
        yield line.split(separator)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    
    for request in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimitedp
        

        if request[0] != '' and request[0] != 'APP_AID' and request[0] != 'c3ac59b8ed434a4830a65157d4d132d3aab9781f':
        
            print "%s%s%s%s%s%s%s%s%s%s%s" % (request[0],separator,request[1],separator,
            request[2],separator,request[3],separator,request[4],separator,request[5])
  
if __name__ == "__main__":
    main()
