import PyTango

class Notifier():
        """Auxiliar class to help the notification service
        
        This class implements the notification service, that means, it 
        will execute a tango_command or a tango string to notify
        that a new file is ready for processing"""
        def __init__(self,tango_entry):
            self.dev = None
            self.attr = None
	    self.tango_name = tango_entry
        
        def trigger(self,path):
	    try:
		    # Setup proxy if necessary
		    if (not self.dev) and (not self.attr):
        		    if self.tango_name.find('->') >0:
                		name_spl = self.tango_name.split('->')
                		self.dev = (PyTango.DeviceProxy(name_spl[0]),name_spl[1])
				self.Proxy.set_timeout_millis(500)
        		    else:
                		self.attr = PyTango.AttributeProxy(self.tango_name)
		    # Notify
        	    if self.dev:
                	self.dev[0].command_inout(self.dev[1],str(path))
        	    else:
                	self.attr.write(str(path))
            except Exception, ex: 
		    #PyTango.Except.print_exception(ex)
		    print "*** Notification error: Exception on",self.tango_name
		    return False
	    return True
