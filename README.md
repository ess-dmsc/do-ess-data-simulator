DonkiOrchestra as an ESS Data Simulator
=======================================

DonkiOrchestra is a specialised framework that offers advanced WORKFLOW control for beamline end-station software. It has been re-purposed to be used with KAFKA-ESS technologies aiming at providing rapid SCRIPTING of complex scenarios of DATA STREAM generation and processing for SIMULATION purposes.


DESCRIPTION
-----------

The repository consists of two main directories: DonkiDirector and DonkiPlayer.

REFERENCES
-----------

 * NOBUGS 2016: https://indico.esss.lu.se/event/357/session/8/contribution/36
 * ICALEPCS 2017: http://icalepcs2017.vrws.de/papers/thmpa03.pdf

USAGE
-----

*  **Director**

In order to start the DonkiDirector do:
```
cd DonkiDirector
python DonkiDirector_cmdline.py
```
This application accepts user input commands that allows you to setup the players and start a sequence with a defined number of triggers. Available commands are:  
   *  Available commands are:
      * `start` -> start the sequence of triggers 
      *  `stop` -> stops the sequence of triggers
      *  `players?` -> displays the Players added
      *  `triggers=N` -> sets the number of triggers 
      *  `priority[playername]=N` -> sets the level of priority of a Player
      *  `quit` -> quit and exit the application


* **Player**

In order to start a generic DonkiPlayer do:
```
cd DonkiPlayer
python DonkiPlayer.py player_name localhost:50010 scripts/just_action.py
```
You have to specify the arguments `player_name` (a unique name for the player), `localhost:50010` (the URL for the information server) and `action_code` (the script that will run as the sequence of triggers arrives to the Player).

EXAMPLES
--------

* **Simple example**

In one terminal run:
```
cd DonkiDirector
python DonkiDirector_cmdline.py
```
In a separate terminal starts a Player:
```
cd DonkiPLayer
python DonkiPlayer.py player0 localhost:50010 scripts/just_action.py
```
After starting the Player set the desired number of `triggers` and changes the level of `priority` of the Player on Director's terminal:
```
triggers=N
priority[player0]=N
```
The pre-defined level of priority is `0`. In order to disable a Player sets its level of priority to `-1`.
On the the Director's terminal type:
```
start
```
The sequence of triggers will be send and the Player(s) will do the actions pre-defined in `just_action.py`. In order to have multiple Players just run the same commands for the Player above in separate terminals.

* **Kafka example**

Send a sequence of triggers to three Players (*run_player_0.py*, *run_player_1.py*, *run_player_2.py*) and streams data generated by the `mcstas-generator` to three Kafka topics (*test-topic-0*, *test-topic-1*, *test-topic-2*) on the broker "localhost".

  * Steps:
    - starts Kafka server
    - starts DonkiDirector
    - starts DonkiPlayer(s)

You can perform streams of data in `parallel` by setting the Players with the same level of priotiry (e.g. `priority[player0]=0`, `priority[player1]=0`, `priority[player2]=0`) or in `series` by setting each Player with a different level of priority (e.g. `priority[player0]=0`, `priority[player1]=1`, `priority[player2]=2`).
    



