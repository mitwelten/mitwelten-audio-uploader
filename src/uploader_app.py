import os
import sys
import time
import jwt
import sqlite3
import uuid

from shiboken6 import isValid
from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices, QStandardItemModel, QStandardItem, QAction
from PySide6.QtWidgets import QApplication, QMainWindow, QFileDialog
from PySide6.QtNetworkAuth import QOAuth2AuthorizationCodeFlow, QOAuthHttpServerReplyHandler, QAbstractOAuth2

from appdirs import AppDirs

from config import APPNAME, APPAUTHOR, DBFILENAME, MW_AUTHURL, MW_TOKENURL, MW_CLIENTID, MW_AUTHSCOPE
from glue import chunks, RootDevice, RootDeviceSelection
from uploader_app_main_ui import Ui_MainWindow
from uploader_app_index_th import IndexWorker
from uploader_app_meta_th import MetaWorker
from uploader_app_upload_th import UploadWorker

class MitweltenAudioUploaderApp(QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()

        self.appdirs = AppDirs(APPNAME, APPAUTHOR)
        os.makedirs(self.appdirs.user_data_dir, exist_ok=True)
        self.dbpath = os.path.join(self.appdirs.user_data_dir, DBFILENAME)

        self.setupUi(self)
        self.setWindowTitle(f'{APPNAME} ({APPAUTHOR})')

        # local ui setup
        self.extract_upload_ctrl.setEnabled(False)
        self.error_frame.setEnabled(False)
        self.indexing_counter.hide()

        self.rds = RootDeviceSelection(self)
        self.rds.changed.connect(self.checkReady)
        self.rds.reset.connect(self.checkReady)

        self.setupOAuth2()
        self.setupDb()
        self.connectSignalsSlots()

        self.path = None
        self.root_prefix = None
        self.indexWorker = None
        self.metaWorker = None
        self.uploadWorker = None

        self.populateRoots()
        self.dbReadStatus()

        ## Exit QAction
        exit_action = QAction('Exit', self)
        exit_action.triggered.connect(self.close)

    def checkReady(self, rd = None):
        'Logged in and root device selected? Then enable the rest of the GUI'
        ready = bool(self.rds.root_device) and (self.oauth.status() == QAbstractOAuth2.Status.Granted)
        self.extract_upload_ctrl.setEnabled(ready)
        self.error_frame.setEnabled(ready)

    def setupOAuth2(self):
        self.oauth = QOAuth2AuthorizationCodeFlow()
        self.oauth.setAuthorizationUrl(QUrl(MW_AUTHURL))
        self.oauth.setAccessTokenUrl(QUrl(MW_TOKENURL))
        self.oauth.setClientIdentifier(MW_CLIENTID)
        self.oauth.setScope(MW_AUTHSCOPE)

        # Use QOAuthHttpServerReplyHandler for receiving the redirect response from the provider
        reply_handler = QOAuthHttpServerReplyHandler(self)
        self.oauth.setReplyHandler(reply_handler)

        # Connect signals
        reply_handler.callbackReceived.connect(self.authHandler)
        self.oauth.granted.connect(self.authSuccessful)
        self.oauth.error.connect(self.authFailed)
        self.oauth.tokenChanged.connect(self.onAuthTokenChanged)

    def connectSignalsSlots(self):
        self.pushButton_add_root.clicked.connect(self.addRoot)
        self.pushButton_signinout.clicked.connect(self.authInit)
        self.pushButton_index.clicked.connect(self.indexPath)
        self.pushButton_extract.clicked.connect(self.extractMeta)
        self.pushButton_startstop_upload.clicked.connect(self.toggleUploadThread)
        self.pushButton_resume.clicked.connect(self.resumeUploadTasks)
        self.pushButton_pause.clicked.connect(self.pauseUploadTasks)
        self.pushButton_retry.clicked.connect(self.retryUploadTasks)
        self.comboBox_select_root.currentIndexChanged.connect(self.selectRoot)

    # ----- root devices

    def addRoot(self):
        root = str(QFileDialog.getExistingDirectory(self, 'Browse for device/root'))
        root_uuid = None
        if os.path.isdir(root) and os.access(root, os.W_OK) and os.listdir(root):
            rootfilepath = os.path.join(root, '.mitwelten_upload_root')
            if os.path.isfile(rootfilepath):
                with open(rootfilepath, 'r') as rootfile:
                    root_uuid = rootfile.readline().rstrip()
            else:
                root_uuid = str(uuid.uuid4())
                with open(rootfilepath, 'w') as rootfile:
                    rootfile.write(root_uuid)
            c = self.database.cursor()
            r = c.execute('select * from roots where uuid = ?', [root_uuid]).fetchone()
            if r == None:
                c.execute('''insert into roots(uuid, prefix) values(?, ?)''', [root_uuid, root])
                self.database.commit()
                r = c.execute('select * from roots where uuid = ?', [root_uuid]).fetchone()
            else:
                print('root already added')
            c.close()
            self.populateRoots()

        else:
            self.statusbar.showMessage(f'ERROR: Device/root not writable: "{root}"', 5000)

    def populateRoots(self):
        def create_model(roots: list[RootDevice]):
            model = QStandardItemModel()
            item = QStandardItem('(select root device)')
            item.setData(RootDevice((-1,'','','')))
            model.appendRow(item)
            for rd in roots:
                item = QStandardItem(f'{rd.prefix} ({rd.label})')
                item.setData(rd)
                model.appendRow(item)
            return model
        result = self.database.execute('select root_id, uuid, prefix, label from roots order by updated_at desc')
        roots = result.fetchall()
        model = create_model([RootDevice(r) for r in roots])
        self.comboBox_select_root.setModel(model)
        if len(roots):
            self.comboBox_select_root.setCurrentIndex(1)


    def selectRoot(self, index):
        try:
            rd: RootDevice = self.comboBox_select_root.model().item(index).data()
            if rd.root_id == -1:
                self.rds.clearRootDevice()
                return

            root_uuid = None
            if self.checkRootAvailable(rd):
                rootfilepath = os.path.join(rd.prefix, '.mitwelten_upload_root')
                if os.path.isfile(rootfilepath):
                    with open(rootfilepath, 'r') as rootfile:
                        root_uuid = rootfile.readline().rstrip()
            if rd.uuid == root_uuid:
                # self.root_prefix = root
                self.root_prefix = rd.prefix
                self.rds.selectRootDevice(rd)
                self.statusbar.showMessage(f'Selected root: {rd.prefix} ({rd.label})', 3000)
            else:
                raise Exception(f'ERROR: Selected root "{rd.prefix} ({rd.label})" is not available')
        except Exception as e:
            self.statusbar.showMessage(str(e), 5000)
            self.comboBox_select_root.setCurrentIndex(0)

    def checkRootAvailable(self, rd: RootDevice | None = None):
        if not rd: rd = self.rds.root_device
        if (rd  and os.path.isdir(rd.prefix)
                and os.access(rd.prefix, os.R_OK)
                and os.listdir(rd.prefix)):
            return True
        else:
            self.rds.clearRootDevice()
            return False

    # ----- indexing

    def indexPath(self):
        path = str(QFileDialog.getExistingDirectory(self, 'Browse for source folder'))
        if root_id := self.checkPathInRoots(path):
            self.path = path
        else:
            self.statusbar.showMessage(f'ERROR: Path not in any existing root: "{self.path}"', 5000)
            return

        self.statusbar.showMessage(f'Path to index set to "{self.path}"', 3000)
        self.indexWorker = IndexWorker(self.path, root_id)
        self.lcdNumber_indexing.display(0)
        self.indexWorker.countChanged.connect(lambda c: self.lcdNumber_indexing.display(c))
        self.indexWorker.indexFinished.connect(self.insertIndex)
        self.indexWorker.finished.connect(self.indexWorker.deleteLater)
        self.indexWorker.destroyed.connect(self.onIndexWorkerStopped)
        self.indexWorker.started.connect(self.onIndexWorkerStarted)
        self.indexWorker.start()
        self.pushButton_index.setEnabled(False)

    def stopIndexWorker(self):
        self.pushButton_index.setEnabled(False)
        self.indexWorker.cancel()

    def onIndexWorkerStarted(self):
        self.indexing_counter.show()
        # change the button to stop indexing
        self.pushButton_index.clicked.disconnect()
        self.pushButton_index.clicked.connect(self.stopIndexWorker)
        self.pushButton_index.setText('stop indexing')
        self.pushButton_index.setEnabled(True)

    def onIndexWorkerStopped(self):
        self.indexing_counter.hide()
        # change the button to start indexing
        self.pushButton_index.clicked.disconnect()
        self.pushButton_index.clicked.connect(self.indexPath)
        self.pushButton_index.setText('index path')
        self.pushButton_index.setEnabled(True)


    def checkPathInRoots(self, path: str):
        c = self.database.cursor()
        r = c.execute('select root_id, prefix from roots').fetchall()
        c.close()
        for root_id, prefix in r:
            if path.startswith(prefix): return root_id
        return False

    def insertIndex(self, files: list, root_id: int):
        if len(files):
            BATCHSIZE = 1000
            c = self.database.cursor()
            for batch, i in chunks(files, BATCHSIZE):
                msg = f'Inserting paths: {1 + (i // BATCHSIZE)} of {1 + (len(files) // BATCHSIZE)}'
                self.statusbar.showMessage(msg, 500)
                c.executemany('''
                insert or ignore into files(path, state, indexed_at, root_id)
                values (?, 0, strftime('%s'), ?)
                ''',[(path, root_id) for path in batch])
                self.database.commit()
            c.close()
        self.dbReadStatus()

    # ----- metadata extraction
    # TODO: change button show/hide/en-/disable mechanics

    def extractMeta(self):
        if not self.checkRootAvailable(): return
        self.metaWorker = MetaWorker(self.dbpath, self.rds.root_device)
        self.metaWorker.countChanged.connect(self.onMetaCountChanged)
        self.metaWorker.totalChanged.connect(self.onMetaTotalChanged)
        self.metaWorker.metaFinished.connect(self.finishMeta)
        self.metaWorker.start()
        # change the button to stop meta extraction
        self.pushButton_extract.clicked.disconnect()
        self.pushButton_extract.clicked.connect(self.stopMetaWorker)
        self.pushButton_extract.setText('stop extracting')

    def onMetaCountChanged(self, c):
        self.progressBar_extracting.setValue(c)
        self.dbReadStatus()

    def onMetaTotalChanged(self, c):
        self.progressBar_extracting.setMaximum(c)

    def stopMetaWorker(self):
        self.metaWorker.cancel()

    def finishMeta(self):
        self.pushButton_extract.clicked.disconnect()
        self.pushButton_extract.clicked.connect(self.extractMeta)
        self.pushButton_extract.setText('extract meta')
        self.dbReadStatus()

    # ----- upload

    def toggleUploadThread(self):
        if not self.checkRootAvailable(): return
        if not self.uploadWorker or (self.uploadWorker and not isValid(self.uploadWorker)):
            self.startUploadWorker()
        else:
            self.stopUploadWorker()

    # should i pause corresponding tasks when root devices is changed/unset?

    def startUploadWorker(self):
        self.pushButton_startstop_upload.setEnabled(False)
        self.uploadWorker = UploadWorker(self.dbpath, self.rds.root_device, self.oauth.token())
        self.uploadWorker.countChanged.connect(self.onUploadCountChanged)
        self.uploadWorker.uploadPaused.connect(self.onUploadPaused)
        self.uploadWorker.tokenExpired.connect(self.onTokenExpired)
        self.uploadWorker.finished.connect(self.uploadWorker.deleteLater)
        self.uploadWorker.started.connect(self.uploadStartedCb)
        self.uploadWorker.destroyed.connect(self.uploadStoppedCb)
        self.uploadWorker.start()

    def uploadStoppedCb(self):
        self.pushButton_startstop_upload.setEnabled(True)
        self.pushButton_startstop_upload.setText('start uploading')

    def uploadStartedCb(self):
        self.pushButton_startstop_upload.setEnabled(True)
        self.pushButton_startstop_upload.setText('stop uploading')

    def onUploadCountChanged(self):
        self.dbReadStatus()

    def onUploadPaused(self):
        time.sleep(5)
        self.dbReadStatus()

    def onTokenExpired(self):
        print('trying to refresh the access token')
        self.oauth.refreshAccessToken()

    def stopUploadWorker(self):
        if self.uploadWorker:
            self.pushButton_startstop_upload.setEnabled(False)
            self.uploadWorker.cancel()

    def pauseUploadTasks(self):
        self.database.execute('update files set state = 42 where state = 1')
        self.database.commit()
        self.dbReadStatus()

    def resumeUploadTasks(self):
        self.database.execute('update files set state = 1 where state = 42')
        self.database.commit()
        self.dbReadStatus()

    def retryUploadTasks(self):
        self.database.execute('update files set state = 1 where state in (-4, -5, -6, -7, -8)')
        self.database.commit()
        self.dbReadStatus()

    # ----- auth

    def authInit(self):
        # self.dialog.web_view.load(self.oauth.buildAuthenticateUrl())
        # self.dialog.exec()
        QDesktopServices.openUrl(self.oauth.buildAuthenticateUrl())

    def onAuthTokenChanged(self, token):
        if self.uploadWorker:
            print('onAuthTokenChanged, setting token')
            self.uploadWorker.setToken(token)

    def authHandler(self, payload):
        if 'code' in payload:
            self.oauth.grant()

    def authSuccessful(self):
        # print("Authentication successful!")
        # print(f"Access Token: {self.oauth.token()}")
        # self.dialog.close()
        self.pushButton_signinout.setText('Sign out')
        self.pushButton_signinout.hide()
        decoded_payload = jwt.decode(self.oauth.token(), options={"verify_signature": False})
        msg = f'''Signed in as {decoded_payload['name']} ({decoded_payload['preferred_username']})'''
        self.label_signinout.setText(msg)
        self.statusbar.showMessage(msg, 3000)
        self.checkReady()

    def authFailed(self, error: str):
        msg = f'''Authentication error: {error}'''
        self.onAuthTokenChanged(None)
        self.label_signinout.setText(msg)
        self.statusbar.showMessage(msg, 3000)
        self.checkReady()

    def dbReadStatus(self):
        c = self.database.cursor()
        state = c.execute('select state, count(file_id) from files group by state;').fetchall()
        state_slots = {sid: 0 for sid in [-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 42]}
        for s, n in state:
            state_slots[s] = n

        done = 0
        unprocessed = 0
        total = 0
        for s, n in state_slots.items():
            total += n
            if s == -8:
                self.lcdNumber_err_read.display(n)
            elif s == -7:
                self.lcdNumber_err_filenotfound.display(n)
            elif s == -6:
                done += n
                self.lcdNumber_err_deploy.display(n)
            elif s == -5:
                self.lcdNumber_err_metainsert.display(n)
            elif s == -4:
                self.lcdNumber_err_upload.display(n)
            elif s == -3:
                done += n
                self.lcdNumber_err_dbdup.display(n)
            elif s == -2:
                done += n
                self.lcdNumber_err_localdup.display(n)
            elif s == -1:
                self.lcdNumber_err_meta.display(n)
            elif s == 0:
                unprocessed += n
                self.lcdNumber_indexed.display(n)
            elif s == 1:
                self.lcdNumber_ready.display(n)
            elif s == 2:
                ...
                # upload successful, becomes 4 because no delete
            elif s == 3:
                self.lcdNumber_queued.display(n)
            elif s == 4:
                done += n
                self.lcdNumber_done.display(n)
            elif s == 42:
                self.lcdNumber_paused.display(n)
            else:
                print('unknown state', s)
        c.close()

        if total > 0:
            self.progressBar_extracting.setMaximum(total)
            self.progressBar_extracting.setValue(total - unprocessed)
            self.progressBar_uploading.setMaximum(total)
            self.progressBar_uploading.setValue(done)
            self.label_pb_extracting.setText(f'extracted: {100*((total - unprocessed)/total):.2f}%')
            self.label_pb_uploading.setText(f'uploaded: {100*(done/total):.2f}%')
        else:
            self.progressBar_extracting.setMaximum(100)
            self.progressBar_extracting.setValue(0)
            self.progressBar_uploading.setMaximum(100)
            self.progressBar_uploading.setValue(0)
            self.label_pb_uploading.setText(f'extracted: 0%')
            self.label_pb_uploading.setText(f'uploaded: 0%')


    def setupDb(self):
        self.database = sqlite3.connect(self.dbpath)
        c = self.database.cursor()
        c.execute('''create table if not exists files (
            file_id integer primary key,
            sha256 text unique,
            path text unique not null,
            state integer not null,
            timestamp integer,
            node_label text,
            file_size integer,
            audio_format text,
            bit_depth integer,
            channels integer,
            duration real,
            sample_rate integer,
            serial_number text,
            source text,
            gain text,
            filter text,
            amp_thresh text,
            amp_trig text,
            battery text,
            temperature text,
            rec_end_status text,
            indexed_at integer default (strftime('%s', 'now')),
            checked_at integer,
            meta_uploaded_at integer,
            file_uploaded_at integer,
            root_id integer not null
        )''')
        c.execute('create index if not exists files_state_idx on files (state)')
        c.execute('create index if not exists root_id_idx on files (root_id)')
        c.execute('''create table if not exists roots (
            root_id integer primary key,
            uuid text unique NOT NULL,
            prefix text NOT NULL,
            label text,
            created_at integer default (strftime('%s', 'now')),
            updated_at integer default (strftime('%s', 'now'))
        )''')
        self.database.commit()
        c.close()

    def closeEvent(self, event):
        if self.indexWorker and isValid(self.indexWorker) and self.indexWorker.isRunning():
            self.stopIndexWorker()
            self.indexWorker.wait()
        if self.metaWorker and isValid(self.metaWorker) and self.metaWorker.isRunning():
            self.stopMetaWorker()
            self.metaWorker.wait()
        if self.uploadWorker and isValid(self.uploadWorker) and self.uploadWorker.isRunning():
            self.stopUploadWorker()
            self.uploadWorker.wait()
        super(QMainWindow, self).closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MitweltenAudioUploaderApp()
    window.show()
    sys.exit(app.exec())
