from tqdm import tqdm

from config import load_drive_service, EXTERN_FOLDER_ID, RECHECK_FOLDER_ID

FROM_FOLDE_ID = RECHECK_FOLDER_ID
TO_FOLDE_ID = EXTERN_FOLDER_ID


def get_files_in_folder(service, folder_id):
    files = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"
    with tqdm(desc=f"Lese Dateien in Ordner {folder_id}", unit="Seite") as bar:
        while True:
            response = service.files().list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, md5Checksum)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=1000,
                pageToken=page_token
            ).execute()
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            bar.update(1)
            if not page_token:
                break
    return files


def move_file_to_folder(service, file_id, target_folder_id):
    file = service.files().get(fileId=file_id, fields="parents").execute()
    previous_parents = ",".join(file.get("parents", []))
    service.files().update(
        fileId=file_id,
        addParents=target_folder_id,
        removeParents=previous_parents,
        fields="id, parents"
    ).execute()


def delete_file(service, file_id):
    service.files().delete(fileId=file_id).execute()


def main():
    service = load_drive_service()

    from_files = get_files_in_folder(service, FROM_FOLDE_ID)
    to_files = get_files_in_folder(service, TO_FOLDE_ID)
    to_md5_set = {f["md5Checksum"] for f in to_files if "md5Checksum" in f}

    moved, deleted = 0, 0

    with tqdm(total=len(from_files), desc="Verarbeite From-Dateien", unit="Datei") as bar:
        for file in from_files:
            file_id = file["id"]
            md5 = file.get("md5Checksum")
            if not md5:
                bar.update(1)
                continue

            if md5 in to_md5_set:
                delete_file(service, file_id)
                tqdm.write(f"[🗑] {file['name']} gelöscht (bereits in To)")
                deleted += 1
            else:
                move_file_to_folder(service, file_id, TO_FOLDE_ID)
                tqdm.write(f"[→] {file['name']} verschoben nach To")
                moved += 1
            bar.update(1)

    tqdm.write(f"Fertig. {moved} verschoben, {deleted} gelöscht.")


if __name__ == "__main__":
    main()
