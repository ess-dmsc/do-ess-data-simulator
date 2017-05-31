# -*- coding: utf-8 -*-
"""
The xml file must provide a list of **attributes**, for example::
   
   <attribute type='sync'>
    <name>image/i16</name>
    <tango_attr>test/rtdevice/test/Image16</tango_attr>
    <metadata name='height' type='static'>
      test/rtdevice/test/Height
    </metadata>
    <metadata name='width' type = 'dynamic'>
	test/rtdevice/test/Width
    </metadata>        
  </attribute>

There are two kinds of attribute, the *sync* and the *meta* types. 

The *sync* attributes are those that are acquired synchronously with the Fermi Shot, and
receives the tag called *bunch number*. It will compose the HDF5 dataset.

The *sync* attribute has the following entries:

* **name**: Identification and the entry for the HDF5 file -> (dataset=i16, group=image).
* **tango_attr**: The tango source that the the Fermidaq system needs to know how to collect this data. Usually, is a tango attribute.
* **metadata**: Information related to this dataset, that will be collected as metadata. It is a tango attribute. Each metadata must provide the following fields: 
   * **name**: name of the metadata key. For example: unit, description, exposure time of a digitizer, etc... 
   * **type**: *dynamic* or *static*[default]. Define the nature of this metadata. *Statics* metadata will be read just once for the whole acquisition, while *dynamic* metadata will be read every time a new HDF5 is to be concluded. 

The *meta* attributes, is similar, but will produce datasets for the HDF5 files. 


It is possible to include information contained in another xml file using the 'xml_include', for example:

 <xml_include>
    <path>config/included_cfg.xml</path>
 </xml_include>

* **path: fullpath of a config XML file to include

"""
dbg_ = False    
import sys
from xml.dom import minidom
from UserDict import UserDict
from collections import namedtuple
import traceback

class InvalidFileError(Exception): 
    """Exception raised for wrong xml files that could not be 
    transformed in DaqXmlConfig object."""    
    pass


import urllib                         
import StringIO

def open_anything(source):            
    """URI, filename, or string --> stream

    This function lets you define parsers that take any input source
    (URL, pathname to local or network file, or actual data as a string)
    and deal with it in a uniform manner.  Returned object is guaranteed
    to have all the basic stdio read methods (read, readline, readlines).
    Just .close() the object when you're done with it.
    
    Examples::
        
        >>> from xml.dom import minidom
        >>> sock = open_anything("http://localhost/kant.xml")
        >>> doc = minidom.parse(sock)
        >>> sock.close()
        >>> sock = open_anything("c:\\inetpub\\wwwroot\\kant.xml")
        >>> doc = minidom.parse(sock)
        >>> sock.close()
        >>> sock = open_anything("<ref id='conjunction'><text>and</text><text>or</text></ref>")
        >>> doc = minidom.parse(sock)
        >>> sock.close()
    """
    if hasattr(source, "read"):
        return source

    if source == '-':
        return sys.stdin

    # try to open with urllib (if source is http, ftp, or file URL)

    try:                                  
        return urllib.urlopen(source)     
    except (IOError, OSError):            
        pass                             
    
    # try to open with native open function (if source is pathname)
    try:                                  
        return open(source)               
    except (IOError, OSError):            
        pass
    
    # treat source as string
                           
    return StringIO.StringIO(str(source))

def _get_text(nodelist):
    """ Extract an string from a xml node data."""        
    rc_ = [node.data for node in nodelist 
                if node.nodeType == node.TEXT_NODE]                
    return ''.join(rc_).strip().encode('ascii')

def _get_value(node, tagname):
    """ Extract the value from an xml node"""
    values = node.getElementsByTagName(tagname)
    if len(values) == 0:
        return ''
    internal_node = values[0]
    return _get_text(internal_node.childNodes)



XmlSyncDataItem = namedtuple("XmlSyncDataItem",
                             "tango_attr metadata enable")
XmlMetaDataItem = namedtuple("XmlMetaDataItem",
                             "tango_attr dynamic")
            
class XmlSyncData(UserDict):
    """ Dictionary of ::XmlSyncDataItem.
    
    It ensures that this dictionary contains only valid ::XmlSyncDataItem.
    """
    def __init__(self):
        UserDict.__init__(self)
    def __setitem__(self, key, val):
        assert isinstance(val, XmlSyncDataItem)
        UserDict.__setitem__(self, key, val)    

class XmlMetaData(UserDict):
    """ Dictionary of ::XmlMetaDataItem.
    
    It ensured that this dictionary contains only valid ::XmlMetaDataItem.
    """
    def __init__(self):
        UserDict.__init__(self)
    def __setitem__(self, key, val):
        assert isinstance(val, XmlMetaDataItem)
        UserDict.__setitem__(self, key, val)
        
    
class DaqXmlConfig:
    """ Parser of the XML file in python usefull class
   
    Following, it is shown a xml file example:: 

        <!DOCTYPE fermidaqxml>
        <xml>
        <attribute type='sync'>
            <name>acq</name>
            <tango_attr>test/rtdevice/1/scalar</tango_attr>
        </attribute>
        <attribute type='sync'>
            <name>acquisitioncount</name>
            <tango_attr>test/rtdevice/1/scalar</tango_attr>
            <metadata name='long_scalar' type='static'>
            test/device/1/long_scalar
            </metadata>
            <metadata name='short_scalar' type = 'dynamic'>
            test/device/1/short_scalar
            </metadata>        
        </attribute>
        <attribute type = 'meta'>
            <name>area</name>
            <tango_attr>test/device/1/short_scalar</tango_attr>
        </attribute>    
        
        <xml_include>
            <path>config/included_cfg.xml</path>
        </xml_include>    
        
	</xml>
        
    
    A section of one or more attribute: 
    
    An attribute may be: 
        - sync: It will be acquired for each bunch number.
            - name: the name correlated to the hdf dataset entry
            - tango_attr: the tango attribute type
            - One or more metadata entries that will be used as metadata
              for the HDF5 dataset. It may be static - will be acquired 
              just once at all, or dynamic, it will be acquired at the end
              of each file.
              - name: the name of the metadata
              - tango_attr: tango attribute name to acquire the data
        - meta: work as the metadata inside the attributes. But it is a dataset inside the HDF5 file.
            - name: the name of the dataset. 
            - tango_attr: tango attribute name to acquire the data. 
            - type: dynamic or static: acquired once or at the end of each file acquisition. 
            
    An xml_include tag has one mandatory field:
      - path: fullpath of a config XML file to include
      
    """
    def __init__(self, filename):
        self.sync = XmlSyncData()
        self.meta = XmlMetaData()
        self.doc = self._load(filename)
        self._parse()
    
    def __str__(self):
        return "DaqXmlConfig %s %s"% (self.sync, self.meta)
        
    def _parse(self):
        """Parse the xml file"""
        try:
            attributes = self.doc.getElementsByTagName("attribute")
            xml_includes = self.doc.getElementsByTagName("xml_include")
            self.handle_attributes(attributes)            
            self.handle_xml_includes(xml_includes)
        except IndexError:
            traceback.print_exc()
            raise InvalidFileError(
                    "Failed to parse the configuration file")        
    @classmethod   
    def _load(cls, source):
        """load XML input source, return parsed XML document
        
        - a URL of a remote XML file ("http://diveintopython.org/kant.xml")
        - a filename of a local XML file ("~/diveintopython/common/py/kant.xml")
        - standard input ("-")
        - the actual XML document, as a string
        """
        sock = open_anything(source)
        try:        
            xmldoc = minidom.parse(sock).documentElement
        except Exception, ex:
            raise InvalidFileError(str(ex))
        sock.close()
        return xmldoc
            
                            
    def handle_xml_includes(self, xml_includes):
        """Extract the context information from xml file."""
        for xmlfile in xml_includes:
            self.handle_xml_include(xmlfile)

    def handle_xml_include(self, xmlfile):
        filename = _get_value(xmlfile,'path')
        self.doc = self._load(filename)
        self._parse()

    def handle_attributes(self, attributes):
        """Extract the attributes information from xml file."""
        for attrib in attributes:
            self.handle_attribute(attrib)
                
    def handle_attribute(self, attrib):
        """Extract the specific attribute information from xml file.         
        """
        name_ = _get_value(attrib,'name')
        try:
            attrib_ = _get_value(attrib,'tango_attr')
        except :
            raise InvalidFileError("Invalid entry for attrib: %s"%(name_))

        if attrib.getAttribute('type') == 'sync':
            meta = attrib.getElementsByTagName("metadata")            
            metadata = self.handle_metadata(meta)
            self.sync[name_] = XmlSyncDataItem(attrib_, metadata, True)
        elif attrib.getAttribute('type') == 'meta':
            self.meta[name_] = XmlMetaDataItem(attrib_, False)
        else:
            raise InvalidFileError("""There is no attribute support for 
                type = %s"""%(str(attrib.getAttribute('type'))))
                
    def handle_metadata(self, meta):
        """
        Extract the Metadata information from xml file. 
        
        Inside the attribute field, it is possible to have
        many metadata fields. This method will build a dictionary 
        for these fields, optionaly they may have a type.::
            
            <attribute>
            ...
            <metadata name = 'met_name'>tango_attribute</metatada>
            <metadata name = 'met_name2'>tango_attribute2</metatada>            
            </attribute>
        
        """
        aux = XmlMetaData()
        for meta_attrib in meta:
            (name, value, dynamic) = self.handle_metadata_entry(meta_attrib)
            aux[name] = XmlMetaDataItem(value, dynamic)
        return aux
    
    @classmethod
    def handle_metadata_entry(cls, meta_attrib):
        """Extract the name and the tango attribute from the 
        metadata entry. 

        <metadata name = 'meta_name' [type=[static,dynamic]]>
        tango_attribute
        </metadata>
                
        """
        name = meta_attrib.attributes['name'].value
        tango_attr_name = _get_text(meta_attrib.childNodes)
        dyn = False
        if meta_attrib.getAttribute('type') == 'dynamic':
            dyn = True
        return (name, tango_attr_name, dyn)

    def daq_keys(self):
        """
        Return the information necessary to construct the Sync Attributes. 
        
        It returns the information through a dictionary with the following 
        structure:
        
        - key: dictionary key
            - Tango attribute name
            - FermiDaq acquisition class name, that is used to construct dynamically the object based on the class name. Examples: ScalarFermiDaq, ImageFermiDaq, etc. 
        """
        aux = dict()
        for (daq_key, daq_entry) in self.sync.items():
            if daq_entry.enable:
                aux[daq_key] = (daq_entry.tango_attr)
        return aux
    
    @staticmethod
    def from_daq_keys(daq_keys, bunch_source):
        """ Construct a DaqXmlConfig from a daq_key entry. 
        
        @see ::daq_keys
        """
        
        standard_xmlconfig = """
        <!DOCTYPE fermidaqxml>
        <xml>
        </xml>
        """% bunch_source
        if dbg_: print 'from daq dkeys = ', daq_keys
        daq = DaqXmlConfig(standard_xmlconfig)        
        for (daq_key, daq_value) in daq_keys.items():
            if not isinstance(daq_value, tuple) or len(daq_value) < 2:
                if dbg_: print daq_value
                raise InvalidFileError("""daq_keys should contain at least 
                tango_attribute name and the class for the acquisition""")
            daq.sync[daq_key] = XmlSyncDataItem(daq_value[0], 
                    daq_value[1], dict(), True)
        return daq

READ_ONLY = 0
READ_WRITE = 1
WRITE_ONLY = 2     
IS_COMMAND = 3

class XmlItem:

    def __init__(self, flag , type, handler, name, tango_name, 
                 description="", example="", dimension = 1, memorized = 0 ):
        self.flag = flag
        self.type = type
        self.handler = handler
        self.tango_name = tango_name
        self.name = name
        self.description = description
        self.example = example
        self.dimension = dimension
        self.memorized = memorized
    
    def __str__(self):
        return "Attr %s = %s/%s \n %s \n %s %d"% (self.name, self.handler, 
                                    self.name, self.description, self.example, 
                                    self.memorized)

class ScriptXmlConfig:
    """
    Parser of the XML file in python usefull class
    
    attributes a dict with: 
        - flag: read_only, write_only, read_write
        - type: short, double, string, 
        - handler: tango attribute associated
        - name: name or label of this attribute
        - description: information for the attribute
        - example of usage: example.
                
    """
    def __init__(self, filename):
        self.entries = dict()
        self.doc = self._load(filename)
        self._parse()
    
    def __str__(self):
        return "DaqXmlConfig %s %s"% (str(self.attributes))
        
    def _parse(self):
        """Parse the xml file"""
        try:
            self.handle_entries(self.doc.getElementsByTagName("ATTRIBUTES_RO"),flag=READ_ONLY)
            self.handle_entries(self.doc.getElementsByTagName("ATTRIBUTES_RW"),flag=READ_WRITE)
            self.handle_entries(self.doc.getElementsByTagName("ATTRIBUTES_WO"),flag=WRITE_ONLY)
            self.handle_entries(self.doc.getElementsByTagName("COMMANDS"),flag=IS_COMMAND)
        except IndexError:
            traceback.print_exc()
            raise InvalidFileError(
                    "Failed to parse the configuration file")        
    @classmethod   
    def _load(cls, source):
        """load XML input source, return parsed XML document
        
        - a URL of a remote XML file ("http://diveintopython.org/kant.xml")
        - a filename of a local XML file ("~/diveintopython/common/py/kant.xml")
        - standard input ("-")
        - the actual XML document, as a string
        """
        sock = open_anything(source)
        try:        
            xmldoc = minidom.parse(sock).documentElement
        except Exception, ex:
            raise InvalidFileError(str(ex))
        sock.close()
        return xmldoc
    
    def handle_entries(self, elements, flag):
        
        for group_of_attributes in elements:
            attributes = [attribute 
                            for attribute 
                            in group_of_attributes.childNodes if 
                            attribute.nodeType == 1]
            for attribute in attributes:
                type = _get_value(attribute, 'TYPE')
                handler = _get_value(attribute,'HANDLER')
                attr = handler +'/' + _get_value(attribute, 'TANGONAME')
                name = attribute.nodeName
                desc =_get_value(attribute, 'DESCRIPTION')
                example = _get_value(attribute, 'EXAMPLE')
                dimension_s = _get_value(attribute, 'DIMENSION')
                if dimension_s == '':
                    dimension = 1
                elif dimension_s.find('(')>= 0:
                    dimension = tuple(dimension_s)
                else:
                    dimension = int(dimension_s)                
                memorized_s = _get_value(attribute, 'MEMORIZED')
                if memorized_s == '1' and dimension == 1 :
                    memorized = 1
                else :
                    memorized = 0
                attr_entry = XmlItem(flag, type, handler, name, attr, desc, example, dimension, memorized)
                if dbg_: print attr_entry
                self.entries[attr_entry.name] = attr_entry

                
                

if __name__ == "__main__":
    xml = DaqXmlConfig("config/config_test.xml")
