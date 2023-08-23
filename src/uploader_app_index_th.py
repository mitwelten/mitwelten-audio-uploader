import os
import mimetypes
import re
import traceback

from PySide6.QtCore import QThread, Signal as pyqtSignal

class IndexWorker(QThread):

    countChanged = pyqtSignal(int)
    indexFinished = pyqtSignal(list, int)

    def __init__(self, basepath: str, root_id: int):
        QThread.__init__(self)
        self.basepath = basepath
        self.semaphore = True
        self.root_id = root_id

    def cancel(self):
        self.semaphore = False

    def run(self):
        filepaths = []
        for root, dirs, files in os.walk(os.fspath(self.basepath)):
            if not self.semaphore:
                filepaths = [] # empty the list
                break
            for file in files:
                if not self.semaphore:
                    filepaths = [] # empty the list
                    break
                filepath = os.path.abspath(os.path.join(root, file))
                try:
                    m = re.match(r'\d{8}_\d{6}T?\.(wav|WAV)', file)
                    if m == None:
                        continue
                    file_type = mimetypes.guess_type(filepath)
                    if os.path.basename(filepath).startswith('.'):
                        continue
                    elif file_type[0] == 'audio/x-wav' or file_type[0] == 'audio/wav':
                        filepaths.append(filepath)
                        self.countChanged.emit(len(filepaths))
                    else:
                        continue
                except:
                    print(traceback.format_exc())
        self.indexFinished.emit(filepaths, self.root_id)
        self.quit()
