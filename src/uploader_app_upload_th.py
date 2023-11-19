from datetime import datetime
import time
import jwt
import requests
import traceback
import sqlite3
import logging

from PySide6.QtCore import QThread, Signal as pyqtSignal
from multiprocessing.pool import ThreadPool
from threading import Semaphore, Condition, Lock
from queue import Queue, Empty as QueueEmpty
from requests.auth import AuthBase
from minio import Minio
from minio.commonconfig import Tags

from config import COLS, APIRUL
from glue import (
    is_readable_file, store_task_state, MetadataValidationException,
    MetadataInsertException, ShutdownRequestException, RootDevice
)

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
        'OAuth token'

        self.is_token_valid = True
        '`is_token_valid` flag indicates whether the OAuth token is currently valid'

        self.semaphore = True
        self.session  = requests.Session()

        self.feed_semaphore = Semaphore(1)
        '`feed_semaphore` semaphore blocks the feeding of new tasks into the queue when the token is being refreshed'

        self.token_condition = Condition()
        '`token_condition` condition variable is used to notify all waiting threads when the token has been refreshed'

        self.token_lock = Lock()
        '`token_lock` is used to safely read the shared OAuth token `token`'

    def cancel(self):
        self.semaphore = False

    def setToken(self, token):
        '`setToken` slot, called when token has been refreshed in response to signal `tokenExpired`'
        with self.token_condition:
            if token == None:
                self.semaphore = False
            else:
                self.token = token
                self.is_token_valid = True  # Mark the new token as valid
            self.feed_semaphore.release()  # Unblock the feeding of new tasks
            self.token_condition.notify_all()  # Notify all waiting threads about the new token

    def setRootPrefix(self, root_device: RootDevice):
        self.root_device = root_device

    def run(self):
        c = self.database.cursor()
        NTHREADS = 8
        nthreads_upload = NTHREADS

        try:
            token_expires_at = jwt.decode(self.token, options={"verify_signature": False})['exp']
            if not check_api(token_expires_at):
                logging.warning('token expired, refreshing...')
                self.feed_semaphore.acquire()
                self.tokenExpired.emit()
                with self.token_condition:
                    self.token_condition.wait()
            with self.token_lock:
                token = self.token
            r = self.session.get(f'{APIRUL}/auth/storage', auth=BearerAuth(token))
            r.raise_for_status()
            storage_crd = r.json()
            storage = connect_s3(storage_crd)
        except:
            logging.error(traceback.format_exc())
            logging.warning('closing session')
            self.session.close()
            return

        def upload_worker(queue: Queue):
            VERBOSE = False
            session = requests.Session()
            try:
                conn = sqlite3.connect(self.db, check_same_thread=False)
            except Exception as e:
                logging.error('Task setup failed:', str(e))
                return

            while True:
                record = None
                token = None
                try:
                    record, token = queue.get()
                    with self.token_lock:
                        token = self.token
                except:
                    logging.debug('Exiting thread, queue is empty')
                    return

                if record == None:
                    queue.task_done()
                    return

                # -- testing if bucket exists removed here

                # -- testing login in REST backend removed here

                d = record
                cur = conn.cursor()

                try:
                    # validate record against database
                    r = session.post(f'{APIRUL}/validate/audio',
                        json={ k: d[k] for k in ('sha256', 'node_label', 'timestamp')}, auth=BearerAuth(token))

                    if r.status_code != 200:
                        if r.status_code == 401:
                            self.tokenExpired.emit()
                            logging.error('Access denied, reason:', r.reason)
                        logging.debug(f"failed to validate metadata for {d['path']}: {r.reason}")
                        r.raise_for_status()

                    validation = r.json()
                    if validation['hash_match'] or validation['object_name_match']:
                        h = 'h' if validation['hash_match'] else ''
                        o = 'o' if validation['object_name_match'] else ''
                        cur.execute('update files set state = -3 where file_id = ?', [d['file_id']])
                        conn.commit()
                        raise MetadataValidationException(f"file exists in database ({h}{o}):", d['path'])
                    elif validation['node_deployed'] == False:
                        cur.execute('update files set state = -6 where file_id = ?', [d['file_id']])
                        conn.commit()
                        raise MetadataValidationException('node is/was not deployed at requested time:', d['node_label'], d['timestamp'])
                    else:
                        logging.debug('new file:', validation['object_name'])
                        d['object_name']   = validation['object_name']
                        d['deployment_id'] = validation['deployment_id']

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
                    logging.debug(f'created {upload.object_name}; etag: {upload.etag}')

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
                    logging.debug('inserted metadata into database. done.')

                    # delete file from disk, update state
                    # os.remove(d['path'])

                    # record should not be deleted as the hash is used to check for duplicates
                    cur.execute('''
                    update files set state = 4
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()

                except MetadataValidationException as e:
                    # -3: meta validation error
                    # -6: node not deployed
                    logging.error('MetadataValidationException', str(e))
                    cur.close()
                    queue.task_done()

                except MetadataInsertException as e:
                    # -5: meta insert error
                    logging.error('MetadataInsertException', str(e))
                    cur.execute('''
                    update files set (state, file_uploaded_at) = (-5, strftime('%s'))
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()
                    cur.close()

                except FileNotFoundError:
                    # -7: file not found error
                    # file not found either when uploading or when deleting
                    logging.error('Error during upload, file not found: ', d['path'])
                    cur.execute('''
                    update files set (state, file_uploaded_at) = (-7, strftime('%s'))
                    where file_id = ?
                    ''', [d['file_id']])
                    conn.commit()
                    cur.close()

                except requests.exceptions.HTTPError as e:
                    logging.error('HTTP Error:', str(e))
                    if (e.response.status_code == 401):
                        with self.token_condition:
                            if self.is_token_valid:
                                logging.info('Thread detected session timeout. Refreshing token.')
                                self.is_token_valid = False  # Mark the token as invalid
                                self.feed_semaphore.acquire()  # Block the feeding of new tasks
                                self.tokenExpired.emit() # trigger token refresh
                            else:
                                logging.debug('Thread waiting for token to be refreshed.')
                                self.token_condition.wait()  # Wait for the token to be refreshed
                                logging.debug('Thread refreshed the token.')
                                # If failed, token is None, semaphore is false, worker thread will stop
                    # wait 10sec before trying on the next task
                    time.sleep(10)
                    # mark checked (ready for upload)
                    store_task_state(conn, record['file_id'], 1)

                except requests.exceptions.ConnectionError:
                    logging.error('Connecting Error:', str(e))
                    # wait 10sec before trying on the next task
                    time.sleep(10)
                    # mark checked (ready for upload)
                    store_task_state(conn, record['file_id'], 1)

                except Exception as e:
                    # -4: file upload error
                    logging.error('File upload error:', d['path'], str(e))
                    logging.error(traceback.format_exc())
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
                    logging.info(f"OK: {d['object_name']} <-- {d['path']}")
                finally:
                    queue.task_done()

        # ---- End of worker thread code

        tasks = self.get_tasks()

        try:
            queue = Queue(maxsize=1)
            pool = ThreadPool(nthreads_upload, initializer=upload_worker, initargs=(queue,))

            for task in tasks:
                self.feed_semaphore.acquire()
                queue.put(task)
                self.feed_semaphore.release()
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
                logging.error(traceback.format_exc())

            # close queue and stop worker threads

            logging.debug('signaling threads to stop...')
            for n in range(nthreads_upload):
                queue.put((None, None))

            logging.debug('closing queue...')
            queue.join()

            logging.debug('waiting for tasks to end...')
            pool.close()
            pool.join()
            logging.debug('done.')

        except Exception as e:
            logging.error(traceback.format_exc())

        finally:
            self.session.close()
            self.uploadPaused.emit()
            logging.debug('exiting uploader ctrl thread')
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
                    record['timestamp'] = datetime.utcfromtimestamp(record['timestamp']).isoformat() + 'Z'
                    # check if file is readable. if not, wait
                    # try:
                    #     is_readable_file(record['path'])
                    # except:
                    #     logging.error('file not readable, waiting 600s', record['path'])
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
                    # print('uploader sleeping...', end='\r')
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
                logging.error(traceback.format_exc(), flush=True)
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
    return delta.total_seconds() > 60

