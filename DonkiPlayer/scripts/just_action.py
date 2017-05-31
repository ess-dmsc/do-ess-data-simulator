import time


def get_data_names():
    return []

def setup():
    msg = "Setup done"
    return msg
	
def stop():
    msg = "Stop done"
    return msg
	
def action(val=None):
    print "Action",val,str(time.asctime())
    print "here"
    time.sleep(0.1)
    
    result = {}
    result['playerlog'] = "Action ok"
    return result
