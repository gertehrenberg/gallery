import pickle

from config import GDRIVE_FOLDERS_DICT, load_drive_service

_cached_folder_dict = None


def save_dict(data, file_path=GDRIVE_FOLDERS_DICT):
    with open(file_path, "wb") as f:
        pickle.dump(data, f)


def load_dict(file_path=GDRIVE_FOLDERS_DICT):
    if file_path.exists():
        with open(file_path, "rb") as f:
            return pickle.load(f)
    return {"name_to_id": {}, "id_to_name": {}}


def collect_all_folders(service, parent_id, name_to_id, id_to_name):
    page_token = None
    while True:
        response = service.files().list(
            q=f"mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{parent_id}' in parents",
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()

        for file in response.get("files", []):
            name_to_id[file["name"]] = file["id"]
            id_to_name[file["id"]] = file["name"]
            # Rekursiv auch Unterordner sammeln
            collect_all_folders(service, file["id"], name_to_id, id_to_name)

        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break


def create_folder_dict(service):
    name_to_id = {}
    id_to_name = {}
    root_id = "root"

    collect_all_folders(service, root_id, name_to_id, id_to_name)

    folder_dict = {"name_to_id": name_to_id, "id_to_name": id_to_name}
    save_dict(folder_dict)
    return folder_dict


def folder_name_by_id(folder_id):
    global _cached_folder_dict
    if _cached_folder_dict is None:
        print("[INFO] Lade Folder-Cache aus Datei...")
        _cached_folder_dict = load_dict()
    name = _cached_folder_dict.get("id_to_name", {}).get(folder_id)
    print(f"[LOOKUP] folder_name_by_id('{folder_id}') → '{name}'")
    return name


def folder_id_by_name(folder_name):
    global _cached_folder_dict
    if _cached_folder_dict is None:
        print("[INFO] Lade Folder-Cache aus Datei...")
        _cached_folder_dict = load_dict()
    folder_id = _cached_folder_dict.get("name_to_id", {}).get(folder_name)
    print(f"[LOOKUP] folder_id_by_name('{folder_name}') → '{folder_id}'")
    return folder_id


def main():
    service = load_drive_service()

    # Ordner neu abrufen und speichern
    folder_dict = create_folder_dict(service)

    # Ausgabe: alle Ordner alphabetisch nach Name
    print("\nAlle Ordner:")
    for name in sorted(folder_dict["name_to_id"]):
        print(f"{name}: {folder_dict['name_to_id'][name]}")


if __name__ == "__main__":
    main()

    folder_name_by_id(folder_id_by_name("sex"))
