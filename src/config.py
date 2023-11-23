from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL as asdf

APPNAME = 'Audio Uploader'
APPAUTHOR = 'mitwelten.org'

APPLOGFILE = 'mitwelten_audio_uploader.log'
APPLOGLEVEL = INFO

DBFILENAME = 'audiofile_index.db'

APIRUL ='https://data.mitwelten.org/api/v3'

MW_AUTHURL = 'https://auth.mitwelten.org/auth/realms/mitwelten/protocol/openid-connect/auth'
MW_TOKENURL = 'https://auth.mitwelten.org/auth/realms/mitwelten/protocol/openid-connect/token'
MW_CLIENTID = 'walk'
MW_AUTHSCOPE = 'roles'

BS = 65536

COLS = ['file_id', 'sha256', 'path', 'state', 'timestamp', 'node_label', 'file_size', 'audio_format', 'bit_depth', 'channels', 'duration', 'sample_rate', 'serial_number', 'source', 'gain', 'filter', 'amp_thresh', 'amp_trig', 'battery', 'temperature', 'rec_end_status']

rec_nok_str = {
    'microphone change': 'MICROPHONE_CHANGED',
    'change of switch position': 'SWITCH_CHANGED',
    'switch position change': 'SWITCH_CHANGED',
    'low voltage': 'SUPPLY_VOLTAGE_LOW',
    'magnetic switch': 'MAGNETIC_SWITCH',
    'file size limit': 'FILE_SIZE_LIMITED'
}
