# Mitwelten Audio Uploader

Upload _Audio Moth_ recordings to storage for ML inference tasks.

## Build

Use Nuitka to build the application bundles / binaries.

```bash
python -m nuitka --standalone --macos-create-app-bundle --enable-plugin=pyside6 \
    --nofollow-import-to=IPython --nofollow-import-to=Pillow  --nofollow-import-to=matplotlib \
    src/uploader_app.py
```

## Usage

- Attach storage medium
- Open _Mitwelten Audio Uploader_ application
- Add a _root device_ (a record pointing to your storage medium). This step will add a file to the target medium containg a UUID, for later identification of that storage medium. You can also add a path in you local filesystem as _root device_.
- Select a path for _indexing_: This path has to be a member of on of you _root devices_. This step will scan the selected path recursively for audio moth recordings and create a records for each recording in a local database (stored in you local user profile)
- To continue processing files you need to sign into your mitwelten account, by clicking _sign in_.
- _Metadata_ of _indexed paths_ needs to be extracted first.
- Once you start the _upload_ process, files with extracted metadata (_ready to upload_ ) will be uploaded to remote storage (the remote storage location is determined by the back end).

## Authors and acknowledgment

[mitwelten.org](https://mitwelten.org)

## Project status

This project is actively developed an replaces the application previously implemented as part of the [Machine Learning Backend](https://github.com/mitwelten/mitwelten-ml-backend) ([uploader_app.py](https://github.com/mitwelten/mitwelten-ml-backend/blob/main/ingest/uploader/uploader_app.py))
