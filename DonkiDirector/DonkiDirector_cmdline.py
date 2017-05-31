#!/usr/bin/env python

from DirectorBgnThread import directorThread
import traceback

"""
if __name__ == "__main__":
    dt = directorThread(None)
    dt.start()
    dt.max_triggers = 50
    dt.EnableDataSaving = False
    nFiles = 1
    dt.set_file_prefix("mytest")
    dt.set_file_path(".")
    dt.set_files_contiguous(True)
    dt.set_files_to_save(nFiles)
    dt.set_file_size(dt.max_triggers/nFiles)

    time.sleep(6)
    dt.set_DataAlias("paperino/image","paperino/mandelbrot")
    dt.set_player_priority("paperino",0)
    dt.set_player_priority("pippo",1)
    dt.set_player_priority("pluto",1)

    dt._started = True
    while (dt._started):
        time.sleep(1)
    print "-------------",dt.zcc.ask_for_log("paperino")
    print "-------------",dt.zcc.ask_for_log("pluto")
    print "-------------",dt.zcc.ask_for_log("pippo")
    #dt.join()
    print dt.PlayersInfo

"""
def get_user_input_loop(dt):
        while True:    # infinite loop
            try:
                n = raw_input("\n\nEnter command (type ? for help): ")
                cmd_in = (n.lower()).strip(' ')
                if cmd_in == "start":
                    dt._started = True
                elif cmd_in == "stop":
                    dt._started = False
                elif cmd_in == "players?":
                    print dt.PlayersInfo
                elif "priority[" in cmd_in:
                    plname = (cmd_in.split("[")[1]).split("]")[0]
                    prio = int((cmd_in.split("="))[-1])
                    dt.set_player_priority(plname,prio)
                elif "triggers=" in cmd_in:
                    max_triggers = int(cmd_in.split('=')[-1])
                    dt.max_triggers = max_triggers
                    print "OK"
                elif cmd_in == "quit":
                    return  # stops the loop
                elif cmd_in == "?":
                    print "Available commands:"
                    print "\tstart, stop, players?, triggers=N, priority[plname]=N, quit"
            except KeyboardInterrupt:
                print "Bye"
                return
            except Exception:
                traceback.print_exc()

    

if __name__ == "__main__":
    dt = directorThread(None)
    dt.start()
    #
    dt.max_triggers = 1
    dt.EnableDataSaving = False
    nFiles = 1
    dt.set_file_prefix("mytest")
    dt.set_file_path(".")
    dt.set_files_contiguous(True)
    dt.set_files_to_save(nFiles)
    dt.set_file_size(dt.max_triggers/nFiles)
    #
    get_user_input_loop(dt)
    dt.quit_and_exit()
