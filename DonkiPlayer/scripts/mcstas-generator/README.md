**mcstasGenerator**
===================

Converts a McStas formatted output into an event stream and send it via 0MQ or
Kafka. Instruments can be build combining the different basic detectors: 1-d (ToF), 2d (area), n-d

DESCRIPTION
-----------

The package consists of an "instrument" (*detector.py*, *instrument.py*) and a
"generators" (*zmqGenerator.py*, *kafkaGenerator.py*,*serialiser.py*) section.

The instrument can be built assempling different monitors (in McSats
language). These can be 1-d, 2-d or n-d detectors such as ToF monitors, area
detectors,... Detectors construction require a string containing the filename of the McStas
output. The instrument returns a data stream using ``mcstas2stream`` method. 
The format of each event is described in the ``ds`` field. In general
it contins information on the timestamp (or tof), the hardware status and the pixel ID of each event. 
The number of events in the stream can be increased by means of the ``multiplier`` parameter.

The generator streams data either via Kafka or 0MQ, in raw (header+blob) or Flatbuffer format. It can be controlled from the command line or from file. Available action are "run", "pause", "stop". If "pause" is selected the stream contains only the header, or the corresponding fields in the flatbuffer.

USAGE
-----

Sample usage 
* 0MQ generator, RITA2 instrument
```
python mcstasGeneratorMain.py -a sample/boa.2d -p 1235 -m 10
```
reads the neutron count from the area detector simulated in sample/boa.2d, and send 10 replicas of the events (in the same data blob) using 0MQ on port 1235

* kafka generator, RITA2 instrument
```
python mcstasGeneratorMain.py -a sample/boa.2d -b localhost -t test
```
same as above, but data are transmitted using kafka on the broker "localhost" and topic "test". There is no data multiplication in place

Notes
-----

* The generatorion of python flatbuffer schema is so far experimental. The flatbuffer compiler throws an error if `` -o OUTPUT_PATH/ ``` is not specified.
* flatbuffer is **extremely** slow when packing arrays
