#!/usr/bin/env python
import sys
import time
from PlayerBgndThread  import processThread
sys.path.append('/home/carlos/Documents/Elettra/Kafka_DonkiOrchestra_project/DonkiOrchestra_0.0.0/DonkiPlayer/scripts/mcstas-generator/src')

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print "\nUsage:\n\t",sys.argv[0],"player_name info_server_url action_script\n\t"
        sys.exit(0)
    else:
        player_name = sys.argv[1]
        info_server = sys.argv[2]
        action_code = sys.argv[3]        
   
    dt = processThread(player_name, info_server, action_code)
    try:
        dt.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        dt._alive = False
        print "Bye"


