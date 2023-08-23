from datetime import datetime
import time
import requests
import traceback
import sqlite3

from PySide6.QtCore import QThread, Signal as pyqtSignal
from multiprocessing.pool import ThreadPool
from queue import Queue, Empty as QueueEmpty
from requests.auth import AuthBase
from minio import Minio
from minio.commonconfig import Tags

from config import COLS, APIRUL
from glue import is_readable_file, store_task_state, MetadataInsertException, ShutdownRequestException, RootDevice

class UploadWorker(QThread):

    countChanged = pyqtSignal()
    uploadPaused = pyqtSignal() # queue empty
    tokenExpired = pyqtSignal()

    def __init__(self, db: str, root_device: RootDevice, token: str):
        QThread.__init__(self)
        self.db = db
        self.database = sqlite3.connect(db, check_same_thread=False)
        self.root_device = root_device
        self.token = token
        self.semaphore = True
        self.session  = requests.Session()
        self.session.auth = BearerAuth(self.token)

    def cancel(self):
        self.semaphore = False

    def setToken(self, token):
        self.token = token
        self.session.auth = BearerAuth(self.token)

    def setRootPrefix(self, root_device: RootDevice):
        self.root_device = root_device

    def run(self):
        c = self.database.cursor()
        NTHREADS = 8
        nthreads_upload = NTHREADS

        try:
            r = self.session.get(f'{APIRUL}/login')
            r.raise_for_status()
            token_expires_at = r.json()['exp']
            if not check_api(token_expires_at):
                self.tokenExpired.emit()

            r = self.session.get(f'{APIRUL}/auth/storage')
            r.raise_for_status()
            storage_crd = r.json()
            storage = connect_s3(storage_crd)
        except:
            self.session.close()
            return

        def upload_worker(queue: Queue):
            VERBOSE = False
            session = requests.Session()
            try:
                conn = sqlite3.connect(self.db, check_same_thread=False)
            except Exception as e:
                print('Task setup failed:', str(e))
                return

            while True:
                record = None
                token = None
                try:
                    record, token = queue.get()
                except:
                    print('Exiting thread, queue is empty')
                    return

                if record == None:
                    queue.task_done()
                    return

                # -- testing if bucket exists removed here

                # -- testing login in REST backend removed here

                d = record
                cur = conn.cursor()

                # validate record against database
                try:
                    r = session.post(f'{APIRUL}/validate/audio',
                        json={ k: d[k] for k in ('sha256', 'node_label', 'timestamp')}, auth=BearerAuth(token))

                    if r.status_code != 200:
                        if r.status_code == 401:
                            self.tokenExpired.emit()
                            print('Token is expired? Reason:', r.reason)
                        if VERBOSE: print(f"failed to validate metadata for {d['path']}: {validation['detail']}")
                        r.raise_for_status()

                    validation = r.json()
                    if validation['hash_match'] or validation['object_name_match']:
                        h = 'h' if validation['hash_match'] else ''
                        o = 'o' if validation['object_name_match'] else ''
                        cur.execute('update files set state = -3 where file_id = ?', [d['file_id']])
                        conn.commit()
                        raise Exception(f"file exists in database ({h}{o}):", d['path'])
                    elif validation['node_deployed'] == False:
                        cur.execute('update files set state = -6 where file_id = ?', [d['file_id']])
                        conn.commit()
                        raise Exception('node is/was not deployed at requested time:', d['node_label'], d['timestamp'])
                    else:
                        if VERBOSE: print('new file:', validation['object_name'])
                        d['object_name']   = validation['object_name']
                        d['deployment_id'] = validation['deployment_id']

                except Exception as e:
                    print('Validation failed:', str(e))
                    cur.close()
                    queue.task_done()
                    continue

                # upload procedure
                try:
                    # upload to minio S3
                    tags = Tags(for_object=True)
                    tags['node_label'] = str(d['node_label'])
                    upload = storage.fput_object(storage_crd['bucket'], d['object_name'], d['path'],
                        content_type='audio/wav', tags=tags)

                    # store upload status
                    cur.execute('''
                    update files set (state, file_uploaded_at) = (2, strftime('%s'))
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()
                    if VERBOSE: print(f'created {upload.object_name}; etag: {upload.etag}')

                    # store metadata in postgres
                    r = session.post(f'{APIRUL}/ingest/audio', auth=BearerAuth(token),
                        json={ k: d[k] for k in ('object_name', 'sha256', 'timestamp', 'deployment_id', 'duration',
                                                 'serial_number', 'audio_format', 'file_size', 'sample_rate', 'bit_depth',
                                                 'channels', 'battery', 'temperature', 'gain', 'filter', 'source',
                                                 'rec_end_status')})

                    # this should be caught as MetadataInsertException for distinct status
                    r.raise_for_status()

                    # store metadata status
                    cur.execute('''
                    update files set (state, meta_uploaded_at) = (2, strftime('%s'))
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()
                    if VERBOSE: print('inserted metadata into database. done.')

                    # delete file from disk, update state
                    # os.remove(d['path'])

                    # record should not be deleted as the hash is used to check for duplicates
                    cur.execute('''
                    update files set state = 4
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()

                except MetadataInsertException as e:
                    # -5: meta insert error
                    print('MetadataInsertException', str(e))
                    cur.execute('''
                    update files set (state, file_uploaded_at) = (-5, strftime('%s'))
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()
                    cur.close()

                except FileNotFoundError:
                    # -7: file not found error
                    # file not found either when uploading or when deleting
                    print('Error during upload, file not found: ', d['path'])
                    cur.execute('''
                    update files set (state, file_uploaded_at) = (-7, strftime('%s'))
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()
                    cur.close()

                except requests.exceptions.HTTPError as e:
                    print('HTTP Error:', str(e))
                    if (e.response.status_code == 401):
                        self.tokenExpired.emit()
                    # wait 10sec before trying on the next task
                    time.sleep(10)
                    # mark checked (ready for upload)
                    store_task_state(conn, record['file_id'], 1)

                except requests.exceptions.ConnectionError:
                    print('Connecting Error:', str(e))
                    # wait 10sec before trying on the next task
                    time.sleep(10)
                    # mark checked (ready for upload)
                    store_task_state(conn, record['file_id'], 1)

                except Exception as e:
                    # -4: file upload error
                    print('File upload error:', d['path'], str(e))
                    print(traceback.format_exc())
                    cur.execute('''
                    update files set (state, file_uploaded_at) = (-4, strftime('%s'))
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()
                    cur.close()
                    # TODO: implement logger
                    # logger.error(traceback.format_exc())
                    # logger.error('failed uploading: deleting record from db')
                    #
                    # TODO: query = 'DELETE FROM {}.files_image WHERE file_id = %s'.format(crd.db.schema)
                    #
                else:
                    print(f"OK: {d['object_name']} <-- {d['path']}")
                finally:
                    queue.task_done()

        # ---- End of worker thread code

        tasks = self.get_tasks()

        try:
            queue = Queue(maxsize=1)
            pool = ThreadPool(nthreads_upload, initializer=upload_worker, initargs=(queue,))

            for task in tasks:
                queue.put(task)
                if not self.semaphore:
                    raise ShutdownRequestException

        except ShutdownRequestException:
            tasks.close()

            # drain the queue and reset drained tasks
            try:
                while True:
                    task, token = queue.get(True, 1)
                    c.execute('update files set state = 1 where file_id = ?', (task['file_id'],))
                    queue.task_done()
            except QueueEmpty:
                self.database.commit()
            except:
                print(traceback.format_exc())

            # close queue and stop worker threads

            print('signaling threads to stop...')
            for n in range(nthreads_upload):
                queue.put((None, None))

            print('closing queue...')
            queue.join()

            print('waiting for tasks to end...')
            pool.close()
            pool.join()
            print('done.')

        except Exception as e:
            print(traceback.format_exc())

        finally:
            self.session.close()
            self.uploadPaused.emit()
            print('exiting uploader ctrl thread')
            self.quit()
            self.exit()

    def get_tasks(self):
        '''
        yield records marked as 'in progress' (status = 3)
        '''

        timer = datetime.now()
        while True:
            file_id = None
            try:
                # record_raw = self.database.execute(f'select {",".join(COLS)} from files where state = 1 and path like ? limit 1', [f'{self.root_device.prefix}%']).fetchone()
                record_raw = self.database.execute(f'select {",".join(COLS)} from files where state = 1 and root_id = ? limit 1', [self.root_device.root_id]).fetchone()
                if record_raw:
                    # transform record_raw to dictionary with colname: value
                    record = {k: record_raw[i] for (i,k) in enumerate(COLS)}

                    # check if file is readable. if not, wait
                    # try:
                    #     is_readable_file(record['path'])
                    # except:
                    #     print('file not readable, waiting 600s', record['path'])
                    #     time.sleep(5) # 600
                    #     continue

                    # mark file as queued
                    file_id = record['file_id']
                    self.database.execute('update files set state = 3 where file_id = ?', [file_id])
                    self.database.commit()
                    if (datetime.now() - timer).seconds > 10:
                        self.countChanged.emit()
                        timer = datetime.now()
                    yield (record, self.token)
                else:
                    print('uploader sleeping...', end='\r')
                    time.sleep(10)
                    if not self.semaphore:
                        break
            except GeneratorExit:
                # reset the last picked up task
                if file_id:
                    self.database.execute('update files set state = 1 where file_id = ?', [file_id])
                    self.database.commit()
                break
            except sqlite3.OperationalError:
                # database is probably locked, try again later
                time.sleep(1)
            except:
                print(traceback.format_exc(), flush=True)
                raise Exception('some other error occurred in generator')

class BearerAuth(AuthBase):
    '''Attaches HTTP Bearer Authentication to the given Request object.'''
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r

def connect_s3(crd) -> Minio:
    # connect to S3 storage
    storage = Minio(crd['host'], access_key=crd['access_key'], secret_key=crd['secret_key'])

    # the documentation states this would be false if bucket doesn't exist
    # but instead an exception is raised: MinioException, code=AccessDenied
    if not storage.bucket_exists(crd['bucket']):
        raise RuntimeError(f'Bucket {crd["bucket"]} does not exist.')

    return storage

def check_api(expires_at: float) -> bool:
    delta = datetime.fromtimestamp(expires_at) - datetime.now()
    return delta.seconds > 60

