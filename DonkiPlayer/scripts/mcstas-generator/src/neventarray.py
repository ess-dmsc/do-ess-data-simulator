import numpy as np

event_t = np.dtype(np.uint64)

def multiplyNEventArray(data,multiplier) :
    return np.tile(data,multiplier)
