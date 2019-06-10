import os
import signal
import zipfile
from io import BytesIO


class GracefulKiller:
    def __init__(self, handle_func):
        self.handle_func = handle_func
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.handle_func()


def zip_dir(directory):
    result = BytesIO()
    dlen = len(directory)
    with zipfile.ZipFile(result, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(directory):
            for name in files:
                full = os.path.join(root, name)
                rel = root[dlen:]
                dest = os.path.join(rel, name)
                zf.write(full, dest)
    return result
