from datetime import datetime
import time
import jwt
import requests
import traceback
import sqlite3
import logging

from PySide6.QtCore import QThread, Signal as pyqtSignal
from multiprocessing.pool import ThreadPool
from threading import Condition, Lock, get_ident
from queue import Queue, Empty as QueueEmpty
from requests.auth import AuthBase
from minio import Minio
from minio.commonconfig import Tags

from config import COLS, APIRUL
from glue import (
    MetadataValidationException,
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

        self.token_condition = Condition()
        '`token_condition` condition variable is used to notify all waiting threads when the token has been refreshed'

        self.token_lock = Lock()
        '`token_lock` is used to safely read the shared OAuth token `token`'

        self.db_lock = Lock()
        '`db_lock` is used to safely write to the shared database connection'

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
            self.token_condition.notify_all()  # Notify all waiting threads about the new token

    def setRootPrefix(self, root_device: RootDevice):
        self.root_device = root_device

    def updateDatabase(self, conn: sqlite3.Connection, sql, data=None):
        try:
            c = conn.cursor()
            if data:
                c.execute(sql, data)
            else:
                c.execute(sql)
            with self.db_lock: # Acquire lock before committing
                conn.commit()
        except sqlite3.OperationalError as e:
            logging.critical(f'database is still locked!!! {str(e)}')
            logging.critical(f'updateDatabase: {traceback.format_exc()}')
        except Exception as e:
            logging.critical('some other error occurred in updateDatabase')
            logging.critical(f'updateDatabase: {traceback.format_exc()}')
        finally:
            c.close()

    def run(self):
        c = self.database.cursor()
        NTHREADS = 8
        nthreads_upload = NTHREADS

        try:
            token_expires_at = jwt.decode(self.token, options={"verify_signature": False})['exp']
            if not check_api(token_expires_at):
                logging.warning('UT: token expired, refreshing...')
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
            logging.error(f'UT: {traceback.format_exc()}')
            logging.warning('UT: closing session')
            self.session.close()
            return

        def upload_worker(queue: Queue):
            VERBOSE = False
            session = requests.Session()
            tid = f'T{get_ident()}'
            try:
                conn = sqlite3.connect(self.db, check_same_thread=False)
            except Exception as e:
                logging.error(f'{tid} Task setup failed: {str(e)}')
                return

            while True:
                record = None
                token = None
                try:
                    record, token = queue.get()
                    logging.debug(f'{tid} got record from queue')
                    with self.token_lock:
                        token = self.token
                except:
                    logging.debug(f'{tid} Exiting thread, queue is empty')
                    return

                if record == None:
                    logging.debug(f'{tid} record is None, exiting thread')
                    queue.task_done()
                    return

                # -- testing if bucket exists removed here

                # -- testing login in REST backend removed here

                logging.debug(f'{tid} record: {record["path"]}')
                d = record

                try:
                    # validate record against database
                    r = session.post(f'{APIRUL}/validate/audio',
                        json={ k: d[k] for k in ('sha256', 'node_label', 'timestamp')}, auth=BearerAuth(token))

                    if r.status_code != 200:
                        if r.status_code == 401:
                            self.tokenExpired.emit()
                            logging.error(f'{tid} Access denied, reason: {r.reason}')
                        logging.debug(f"{tid} failed to validate metadata for {d['path']}: {r.reason}")
                        r.raise_for_status()

                    validation = r.json()
                    if validation['hash_match'] or validation['object_name_match']:
                        h = 'h' if validation['hash_match'] else ''
                        o = 'o' if validation['object_name_match'] else ''
                        self.updateDatabase(conn, 'update files set state = -3 where file_id = ?', [d['file_id']])
                        raise MetadataValidationException(f"file exists in database ({h}{o}):", d['path'])
                    elif validation['node_deployed'] == False:
                        self.updateDatabase(conn, 'update files set state = -6 where file_id = ?', [d['file_id']])
                        raise MetadataValidationException('node is/was not deployed at requested time:', d['node_label'], d['timestamp'])
                    else:
                        logging.debug(f"{tid} new file: {validation['object_name']}")
                        d['object_name']   = validation['object_name']
                        d['deployment_id'] = validation['deployment_id']

                    # upload to minio S3
                    tags = Tags(for_object=True)
                    tags['node_label'] = str(d['node_label'])
                    upload = storage.fput_object(storage_crd['bucket'], d['object_name'], d['path'],
                        content_type='audio/wav', tags=tags)

                    # store upload status
                    self.updateDatabase(conn, "update files set (state, file_uploaded_at) = (2, strftime('%s')) where file_id = ? ", [d['file_id']])
                    logging.debug(f'{tid} created {upload.object_name}; etag: {upload.etag}')

                    # store metadata in postgres
                    r = session.post(f'{APIRUL}/ingest/audio', auth=BearerAuth(token),
                        json={ k: d[k] for k in ('object_name', 'sha256', 'timestamp', 'deployment_id', 'duration',
                                                 'serial_number', 'audio_format', 'file_size', 'sample_rate', 'bit_depth',
                                                 'channels', 'battery', 'temperature', 'gain', 'filter', 'source',
                                                 'rec_end_status')})

                    # this should be caught as MetadataInsertException for distinct status
                    r.raise_for_status()

                    # store metadata status
                    self.updateDatabase(conn, " update files set (state, meta_uploaded_at) = (2, strftime('%s')) where file_id = ? ", [d['file_id']])
                    logging.debug(f'{tid} inserted metadata into database. done.')

                    # delete file from disk, update state
                    # os.remove(d['path'])

                    # record should not be deleted as the hash is used to check for duplicates
                    self.updateDatabase(conn, 'update files set state = 4 where file_id = ?', [d['file_id']])

                except MetadataValidationException as e:
                    # -3: meta validation error
                    # -6: node not deployed
                    logging.error(f'{tid} MetadataValidationException {str(e)}')

                except MetadataInsertException as e:
                    # -5: meta insert error
                    logging.error(f'{tid} MetadataInsertException {str(e)}')
                    self.updateDatabase(conn, "update files set (state, file_uploaded_at) = (-5, strftime('%s')) where file_id = ? ", [d['file_id']])

                except FileNotFoundError:
                    # -7: file not found error
                    # file not found either when uploading or when deleting
                    logging.error(f"{tid} Error during upload, file not found: {d['path']}")
                    self.updateDatabase(conn, "update files set (state, file_uploaded_at) = (-7, strftime('%s')) where file_id = ? ", [d['file_id']])

                except requests.exceptions.HTTPError as e:
                    logging.error(f'{tid} HTTP Error: {str(e)}')
                    if (e.response.status_code == 401):
                        with self.token_condition:
                            if self.is_token_valid:
                                logging.info(f'{tid} Thread detected session timeout. Refreshing token.')
                                self.is_token_valid = False  # Mark the token as invalid
                                self.tokenExpired.emit() # trigger token refresh

                            logging.debug(f'{tid} Thread waiting for token to be refreshed.')
                            self.token_condition.wait()  # Wait for the token to be refreshed
                            logging.debug(f'{tid} Thread refreshed the token.')
                            # If failed, token is None, semaphore is false, worker thread will stop

                    # mark checked (ready for upload)
                    logging.debug(f'{tid} Resetting task for {d["path"]}.')
                    self.updateDatabase(conn, 'update files set state = ? where file_id = ?', [1, d['file_id']])

                except requests.exceptions.ConnectionError:
                    logging.error(f'{tid} Connecting Error: {str(e)}')
                    # wait 10sec before trying on the next task
                    time.sleep(10)
                    # mark checked (ready for upload)
                    self.updateDatabase(conn, 'update files set state = ? where file_id = ?', [1, d['file_id']])

                except Exception as e:
                    # -4: file upload error
                    logging.error(f"{tid} File upload error: {d['path']} {str(e)}")
                    logging.error(f'{tid} {traceback.format_exc()}')
                    self.updateDatabase(conn, "update files set (state, file_uploaded_at) = (-4, strftime('%s')) where file_id = ? ", [d['file_id']])
                    # TODO: implement logger
                    # logger.error(traceback.format_exc())
                    # logger.error('failed uploading: deleting record from db')
                    #
                    # TODO: query = 'DELETE FROM {}.files_image WHERE file_id = %s'.format(crd.db.schema)
                    #
                else:
                    logging.info(f"{tid} OK: {d['object_name']} <-- {d['path']}")
                finally:
                    queue.task_done()

        # ---- End of worker thread code

        tasks = self.get_tasks()

        try:
            queue = Queue(maxsize=1)
            pool = ThreadPool(nthreads_upload, initializer=upload_worker, initargs=(queue,))

            for task in tasks:
                with self.token_condition:
                    if not self.is_token_valid:
                        logging.debug('UT: waiting for token to be refreshed.')
                        self.token_condition.wait()
                        logging.debug('UT: token refreshed.')
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
                logging.error(f'UT {traceback.format_exc()}')

            # close queue and stop worker threads

            logging.debug('UT: signaling threads to stop...')
            for n in range(nthreads_upload):
                queue.put((None, None))

            logging.debug('UT: closing queue...')
            queue.join()

            logging.debug('UT: waiting for tasks to end...')
            pool.close()
            pool.join()
            logging.debug('UT: done.')

        except Exception as e:
            logging.error(f'UT: {traceback.format_exc()}')

        finally:
            self.session.close()
            self.uploadPaused.emit()
            logging.debug('UT: exiting uploader ctrl thread')
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
                    #     logging.error(f"file not readable, waiting 600s {record['path']}")
                    #     time.sleep(5) # 600
                    #     continue

                    # mark file as queued
                    file_id = record['file_id']
                    self.updateDatabase(self.database, 'update files set state = 3 where file_id = ?', [file_id])
                    if (datetime.now() - timer).seconds > 10:
                        self.countChanged.emit()
                        timer = datetime.now()
                    yield (record, self.token)
                else:
                    # print('uploader sleeping...', end='\r')
                    # logging.debug(f'generator: uploader sleeping...')
                    time.sleep(10)
                    if not self.semaphore:
                        break
            except GeneratorExit:
                # reset the last picked up task
                if file_id:
                    self.updateDatabase(self.database, 'update files set state = 1 where file_id = ?', [file_id])
                break
            except sqlite3.OperationalError as e:
                logging.error(f'generator: {str(e)}')
                # database is probably locked, try again later
                time.sleep(1)
            except:
                logging.error(f'generator: {traceback.format_exc()}')
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

