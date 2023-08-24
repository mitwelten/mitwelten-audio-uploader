import audio_metadata
import hashlib
import re
import sqlite3
import traceback

from PySide6.QtCore import QThread, Signal as pyqtSignal
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone, timedelta

from config import BS, rec_nok_str
from glue import chunks, is_readable_file, ShutdownRequestException, RootDevice

def meta_worker(row):
    file_id, path = row
    info = {}

    try:
        # create SHA256 hash
        file_hash = hashlib.sha256()
        with open(path, 'rb') as f:
            fb = f.read(BS)
            while len(fb) > 0:
                file_hash.update(fb)
                fb = f.read(BS)
        info['sha256'] = file_hash.hexdigest()

        # TODO: find sensible defaults if audiofile doesn't contain AudioMoth Data
        meta = audio_metadata.load(path)
        comment = meta.tags.comment[0]

        info['original_file_path'] = path
        info['file_size'] = meta.filesize
        info['audio_format'] = meta.streaminfo.audio_format
        info['bit_depth'] = meta.streaminfo.bit_depth
        info['channels'] = meta.streaminfo.channels
        info['duration'] = meta.streaminfo.duration
        info['sample_rate'] = meta.streaminfo.sample_rate

        if 'text' in comment:
            comment = comment['text']
        # Read the time and timezone from the header
        ts = re.search(r"(\d\d:\d\d:\d\d \d\d/\d\d/\d\d\d\d)", comment)[1]
        tz = re.search(r"\(UTC([-|+]\d+)?:?(\d\d)?\)", comment)
        hrs = 0 if tz[1] is None else int(tz[1])
        mins = 0 if tz[2] is None else -int(tz[2]) if hrs < 0 else int(tz[2])

        info['serial_number'] = re.search(r"by AudioMoth ([^ ]+)", comment)[1]

        extmic = re.search(r'using external microphone', comment)
        if extmic != None:
            info['source'] = 'external'
        else:
            info['source'] = 'internal'

        info['gain'] = re.search(r"at ([a-z-]+) gain", comment)[1]

        # - Band-pass filter with frequencies of 1.0kHz and 192.0kHz applied.
        # LOW_PASS_FILTER x, BAND_PASS_FILTER x y, HIGH_PASS_FILTER x
        bpf = re.search(r"Band-pass filter with frequencies of (\d+\.\d+)kHz and (\d+\.\d+)kHz applied\.", comment)
        lpf = re.search(r"Low-pass filter with frequency of (\d+\.\d+)kHz applied\.", comment)
        hpf = re.search(r"High-pass filter with frequency of (\d+\.\d+)kHz applied\.", comment)

        if bpf != None:
            info['filter'] = f'BAND_PASS_FILTER {bpf[1]} {bpf[2]}'
        elif lpf != None:
            info['filter'] = f'LOW_PASS_FILTER {lpf[1]}'
        elif hpf != None:
            info['filter'] = f'HIGH_PASS_FILTER {hpf[1]}'
        else:
            info['filter'] = 'NO_FILTER'

        timestamp = datetime.strptime(ts, "%H:%M:%S %d/%m/%Y")
        info['timestamp'] = timestamp.replace(tzinfo=timezone(timedelta(hours=hrs, minutes=mins))).timestamp()

        amp_res = re.search(r'Amplitude threshold was ([^ ]+) with ([^ ]+)s minimum trigger duration\.', comment)
        if amp_res != None:
            info['amp_thresh'], info['amp_trig'] = amp_res.groups()
        else:
            info['amp_thresh'], info['amp_trig'] = None, None # for postgres

        # Read the battery voltage and temperature from the header
        info['battery'] = re.search(r"(\d\.\d)V", comment)[1]
        info['temperature'] = re.search(r"(-?\d+\.\d)C", comment)[1]

        # read the remaining comment:
        #
        # !RECORDING_OKAY and ever only one more condition
        #
        # the AM recordings contain comments from several firmware versions!
        # the syntax differs (see test.py for unit tests of the expression):
        # - [old]     Recording cancelled before completion due to
        # - [...]     Recording stopped due to
        # - [current] Recording stopped
        #
        # MICROPHONE_CHANGED
        # - microphone change.
        # - due to microphone change.
        #
        # SWITCH_CHANGED
        # - change of switch position.
        # - switch position change.
        # - due to switch position change.
        #
        # MAGNETIC_SWITCH
        # - by magnetic switch.
        #
        # SUPPLY_VOLTAGE_LOW
        # - low voltage.
        # - due to low voltage.
        #
        # FILE_SIZE_LIMITED
        # - file size limit.
        # - due to file size limit.
        #
        # - Recording stopped due to switch position change.
        # - Recording cancelled before completion due to low voltage.
        #
        rec_nok = re.search(r" Recording (?:cancelled before completion|stopped) (?:by|due to) (magnetic switch|microphone change|change of switch position|switch position change|low voltage|file size limit)\.", comment)
        if rec_nok != None:
            info['rec_end_status'] = rec_nok_str[rec_nok[1]]
        else:
            info['rec_end_status'] = 'RECORDING_OKAY'

        # infer node name from path
        m = re.match(r'.*(\d{4}-\d{4}).+', path)
        if m == None:
            raise Exception('Node label extraction failed', info['sha256'], datetime.utcfromtimestamp(info['timestamp']).isoformat() + 'Z', info['serial_number'])
        info['node_label'] = m.groups()[0]

    except Exception as e:
        print(f'{path}: {e}')
        info = {}
    finally:
        info['file_id'] = file_id
        info['path'] = path
    return info

class MetaWorker(QThread):

    countChanged = pyqtSignal(int)
    totalChanged = pyqtSignal(int)
    metaFinished = pyqtSignal()

    def __init__(self, db: str, root_device: RootDevice):
        QThread.__init__(self)
        self.database = sqlite3.connect(db, check_same_thread=False)
        self.root_prefix = root_device.prefix
        self.semaphore = True

    def cancel(self):
        self.semaphore = False

    def run(self):
        c = self.database.cursor()
        NTHREADS = 2
        BATCHSIZE = 32
        nthreads_meta = NTHREADS

        try:
            records = c.execute('select file_id, path from files where sha256 is null and state = 0 and path like ?', [self.root_prefix + '%']).fetchall()
            self.totalChanged.emit(len(records))
            for batch, i in chunks(records, BATCHSIZE):
                if not self.semaphore:
                    raise ShutdownRequestException
                # check if file is readable. if not, skip to waiting for next iteration
                try:
                    # rudimentary check if HDD is lost.
                    # problem: if the file is not there for other reasons, it stops the whole process anyways
                    is_readable_file(batch[0][1])
                except:
                    break

                # print(f'processing batch {1 + (i // BATCHSIZE)} of {1 + (len(records) // BATCHSIZE)} ({BATCHSIZE} items)')
                metalist = []
                # Using ProcessPool instread of ThreadPool saves a few seconds
                with ProcessPoolExecutor(nthreads_meta) as executor:
                    metalist = executor.map(meta_worker, batch)

                for meta in metalist:
                    try:
                        if len(meta) == 2:
                            # meta only contains file_id and path == extraction failed
                            raise ValueError
                        c.execute('''
                        update files set (sha256, state, timestamp, node_label, file_size, audio_format, bit_depth, channels, duration, sample_rate, serial_number, source, gain, filter, amp_thresh, amp_trig, battery, temperature, rec_end_status, checked_at)
                        = (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s'))
                        where file_id = ?
                        ''', [
                            meta['sha256'],
                            meta['timestamp'],
                            meta['node_label'],
                            meta['file_size'],
                            meta['audio_format'],
                            meta['bit_depth'],
                            meta['channels'],
                            meta['duration'],
                            meta['sample_rate'],
                            meta['serial_number'],
                            meta['source'],
                            meta['gain'],
                            meta['filter'],
                            meta['amp_thresh'],
                            meta['amp_trig'],
                            meta['battery'],
                            meta['temperature'],
                            meta['rec_end_status'],
                            meta['file_id']])
                    except ValueError as e:
                        c.execute('''
                        update files set (state, checked_at) = (-1, strftime('%s'))
                        where file_id = ?
                        ''', [meta['file_id']])
                    except sqlite3.IntegrityError as e:
                        print(meta['path'], e)
                        c.execute('''
                        update files set (state, timestamp, node_label, file_size, audio_format, bit_depth, channels, duration, sample_rate, serial_number, source, gain, filter, amp_thresh, amp_trig, battery, temperature, rec_end_status, checked_at)
                        = (-2, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s'))
                        where file_id = ?
                        ''', [
                            meta['timestamp'],
                            meta['node_label'],
                            meta['file_size'],
                            meta['audio_format'],
                            meta['bit_depth'],
                            meta['channels'],
                            meta['duration'],
                            meta['sample_rate'],
                            meta['serial_number'],
                            meta['source'],
                            meta['gain'],
                            meta['filter'],
                            meta['amp_thresh'],
                            meta['amp_trig'],
                            meta['battery'],
                            meta['temperature'],
                            meta['rec_end_status'],
                            meta['file_id']])
                self.database.commit()
                self.countChanged.emit(1 + (i // BATCHSIZE))

        except ShutdownRequestException or KeyboardInterrupt:
            print('ShutdownRequestException, exiting meta extraction')
        except Exception as e:
            print(traceback.format_exc())
            print('meta extraction error occurred, exiting')
        else:
            print('meta extraction done')
        finally:
            print('finally done')
            self.metaFinished.emit()
            c.close()
            self.database.close()
