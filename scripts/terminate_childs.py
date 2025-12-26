import platform
import psutil
import os

from typing import List, Optional
from pathlib import Path

IS_WINDOWS = platform.system() == "Windows"
IS_LINUX = platform.system() == "Linux"


def terminate_childs():
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
                                print(
                                    f"Child worker with PID = {_child.pid} is terminated"
                                )
                            except:
                                pass
                    try:
                        pid = process.pid
                        process.kill()
                        print(f"Granian with PID = {pid} is terminated")
                    except:
                        pass

            # in case of exceptions terminate hanging processes
            if os.path.isfile(file_pids):
                with open(file_pids) as file:
                    pids: List[int] = [
                        int(pid.rstrip()) for pid in file if len(pid.rstrip()) > 0
                    ]
                pids_childs: Optional[List[int]] = None
                if len(pids) > 1:
                    pids_childs = pids[: len(pids) - 1]
                if pids_childs is not None:
                    for process in psutil.process_iter():
                        if process.pid in pids_childs:
                            process.kill()
                            print(
                                f"Worker '{process.name()}' process with PID = {process.pid} is terminated"
                            )
                else:
                    print(f"Childs processes is undefined")
        except Exception as e:
            print(f"Error terminate processes: {e}")


if __name__ == "__main__":
    terminate_childs()
