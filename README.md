** DonkiOrchestra as an ESS Data Simulator **

DonkiOrchestra is a specialised framework that offers advanced WORKFLOW control for beamline end-station software. It has been re-purposed to be used with KAFKA-ESS technologies aiming at providing rapid SCRIPTING of complex scenarios of DATA STREAM generation and processing for SIMULATION purposes.


DESCRIPTION
-----------

The repository consists of tow main directories: DonkiDirector and DonkiPlayer.

USAGE
-----

* Director
In order to start the DonkiDirector do:
```
cd DonkiDirector
./DonkiDirector_cmdline.py
```
This application accepts user input commands that allows you to setup the players and start a sequence with a defined number of triggers. Available commands are:

start, stop, players?, triggers=N, priority[playername]=N, quit

* Player
In order to start a generci DonkiPlayer do:
```
cd DonkiPlayer
./DonkiPlayer.py player_name localhost:50010 scripts/just_action.py
```
You have to specify the arguments ```player_name``` (a unique name for the player), ```localhost:50010``` (the URL for the information server) and ```action_code``` (the script that will run as the sequence of triggers arrives to the Player).






