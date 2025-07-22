import os
import subprocess
import platform

HOME_IP = os.getenv("DATABASE_HOME_HOST")


def on_home_network(home_server_ip=HOME_IP):
    """
    Returns True if the home server IP is reachable (i.e., you're on the home network).
    """
    param = "-n" if platform.system().lower() == "windows" else "-c"
    try:
        result = subprocess.run(
            ["ping", param, "1", home_server_ip],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=3
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except Exception as e:
        print(f"Error checking network status: {e}")
        return False
