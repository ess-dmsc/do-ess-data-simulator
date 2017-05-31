import sys,getopt
import errno
import numpy as np

from detector import ToF
from detector import D2

from instrument import Rita2
                    
if __name__ == "__main__":
    args, _ = getopt.getopt(sys.argv[1:],'t:a:')
    for flag, arg in args:
        if flag=='-t':
            all_arguments = arg.split(',')
            t = [ToF(s) for s in all_arguments]
        if flag=='-a':
            all_arguments = arg.split(',')
            surf = [D2(s) for s in all_arguments]

    t[0].randomize(surf[0].count())

    detector = Rita2(t[0],surf[0])
    stream = detector.mcstas2stream(np.array([2, 1, 1, 1, 1]))

    for i in range(10):
        print stream[i]
    
#    
