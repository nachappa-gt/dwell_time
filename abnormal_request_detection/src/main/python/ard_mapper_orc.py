import sys
def read_input(file,separator = '\t'):
    for line in file:
        ## remove the /n at the end of each line
        ## this is important, otherwise, the mapper will generate a table with an empty row every other row.
        line = line.rstrip()
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
        # tab-delimited
        

        if request[17] != '' and request[17] != 'APP_AID' and request[17] != 'c3ac59b8ed434a4830a65157d4d132d3aab9781f':           
            print separator.join(request)
  
if __name__ == "__main__":
    main()