import platform
import psutil
import os

from typing import List
from pathlib import Path

IS_WINDOWS = platform.system() == "Windows"
IS_LINUX = platform.system() == "Linux"


def terminate():
    cwd = Path(__file__).parent
    file_pids = os.path.join(str(cwd), "PIDs")
    if IS_WINDOWS or IS_LINUX:
        try:
            for process in psutil.process_iter():
                if process.name().find("granian") >= 0:
                    for child in process.children(recursive=True):
                        # For granian childs processes
                        for _child in child.children(recursive=True):
                            try:
                                _child.kill()
                            except:
                                pass
                        try:
                            child.kill()
                        except:
                            pass
                        print(f"Child worker with PID = {child.pid} is terminated")

                    try:
                        pid = process.pid
                        process.kill()
                        print(f"Granian with PID = {pid} is terminated")
                    except:
                        pass

            for process in psutil.process_iter():
                if process.name().find("tiler-server") >= 0:
                    parent_pid = process.pid
                    try:
                        process.kill()
                        print(f"Main server with PID = {parent_pid} is terminated")
                    except:
                        pass

            # in case of exceptions terminate hanging processes
            if os.path.isfile(file_pids):
                with open(file_pids) as file:
                    pids: List[int] = [
                        int(pid.rstrip()) for pid in file if len(pid.rstrip()) > 0
                    ]
                for process in psutil.process_iter():
                    if process.pid in pids:
                        try:
                            process.kill()
                            print(
                                f"Application '{process.name()}' process with PID = {process.pid} is terminated"
                            )
                        except:
                            pass
        except Exception as e:
            print(f"Error terminate processes: {e}")


if __name__ == "__main__":
    terminate()
