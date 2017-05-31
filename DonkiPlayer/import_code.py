# Importing a dynamically generated module
import time

def importCode(code,name,add_to_sys_modules=0):
    """
    Import dynamically generated code as a module. code is the
    object containing the code (a string, a file handle or an
    actual compiled code object, same types as accepted by an
    exec statement). The name is the name to give to the module,
    and the final argument says wheter to add it to sys.modules
    or not. If it is added, a subsequent import statement using
    name will return this module. If it is not added to sys.modules
    import will try to load it in the normal fashion.

    import foo

    is equivalent to

    foofile = open("/path/to/foo.py")
    foo = importCode(foofile,"foo",1)

    Returns a newly generated module.
    """
    import sys,imp

    module = imp.new_module(name)

    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module

    return module

# Example
code = \
"""
import PyTango
dev=PyTango.DeviceProxy('test/device/1')

def read():
    print dev.double_scalar

"""
t0 = time.time()
m = importCode(code,"test")
m.read()
print time.time() - t0
for i in range(10):
	t0 = time.time()
	m.read()
	print time.time() - t0
