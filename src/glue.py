import os
import sqlite3

from PySide6.QtCore import QObject, Signal as pyqtSignal

class IndexingException(BaseException):
    ...

class MetadataValidationException(BaseException):
    ...

class MetadataInsertException(BaseException):
    ...

class ShutdownRequestException(BaseException):
    ...

def chunks(lst, n):
    '''Yield successive n-sized chunks from lst.'''
    for i in range(0, len(lst), n):
        yield lst[i:i + n], i

def is_readable_file(arg):
    os.stat(arg)
    with open(arg, 'rb') as f:
        test = f.read1(8)

def is_readable_dir(arg):
    try:
        if os.path.isfile(arg):
            arg = os.path.dirname(arg)
        if os.path.isdir(arg) and os.access(arg, os.R_OK) and os.listdir(arg):
            return arg
        else:
            raise f'{arg}: Directory not accessible'
    except Exception as e:
        raise ValueError(f'Can\'t read directory/file {arg}')

def store_task_state(conn: sqlite3.Connection, file_id: int, state: int):
    c = conn.cursor()
    c.execute('update files set state = ? where file_id = ?', [state, file_id])
    conn.commit()
    c.close()

class RootDevice(object):

    def __init__(self, record: tuple[int, str, str, str]) -> None:
        root_id, root_uuid, prefix, label = record
        self.root_id = root_id
        self.uuid = root_uuid
        self.prefix = prefix
        self.label = label

class RootDeviceSelection(QObject):

    changed = pyqtSignal(RootDevice)
    reset = pyqtSignal()

    root_device: RootDevice | None

    def selectRootDevice(self, root_device: RootDevice | None):
        self.root_device = root_device
        if self.root_device:
            self.changed.emit(self.root_device)
        else:
            self.reset.emit()

    def clearRootDevice(self):
        self.root_device = None
        self.reset.emit()
