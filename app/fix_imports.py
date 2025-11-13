# !/usr/bin/env python3
"""
Script zum Umwandeln von relativen Imports in app. Imports.
Speichern als: /home/ubuntu/gallery/app/fix_imports.py
Ausführen mit: python fix_imports.py
"""

import re
from pathlib import Path


def convert_relative_to_app(file_path):
    """Konvertiert relative Imports in app. Imports."""

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # Muster für relative Imports
    patterns = [
        # from ..module import x  ->  from app.module import x
        (r'from \.\.(\w+) import', r'from app.\1 import'),

        # from ..package.module import x  ->  from app.package.module import x
        (r'from \.\.(\w+)\.(\w+) import', r'from app.\1.\2 import'),

        # from ..package.subpackage.module import x
        (r'from \.\.(\w+)\.(\w+)\.(\w+) import', r'from app.\1.\2.\3 import'),
    ]

    # Wende alle Muster an
    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content)

    # Prüfe, ob sich etwas geändert hat
    if content != original_content:
        # Erstelle Backup
        backup_path = str(file_path) + '.bak'
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(original_content)

        # Schreibe geänderte Datei
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        return True

    return False


def process_directory(directory):
    """Verarbeitet alle Python-Dateien in einem Verzeichnis rekursiv."""

    app_dir = Path(directory)
    changed_files = []
    skipped_files = []

    # Finde alle .py Dateien
    for py_file in app_dir.rglob('*.py'):
        # Überspringe __pycache__ und .bak Dateien
        if '__pycache__' in str(py_file) or '.bak' in str(py_file):
            continue

        print(f"Verarbeite: {py_file.relative_to(app_dir)}")

        try:
            if convert_relative_to_app(py_file):
                changed_files.append(py_file)
                print(f"  ✅ Geändert")
            else:
                skipped_files.append(py_file)
                print(f"  ⏭️  Keine Änderungen")
        except Exception as e:
            print(f"  ❌ Fehler: {e}")

    return changed_files, skipped_files


def main():
    """Hauptfunktion"""

    app_dir = Path(__file__).parent

    print("=" * 60)
    print("Konvertiere relative Imports (..) zu app. Imports")
    print("=" * 60)
    print(f"Verzeichnis: {app_dir}")
    print()

    # Bestätigung
    response = input("Möchten Sie fortfahren? (j/n): ")
    if response.lower() not in ['j', 'ja', 'y', 'yes']:
        print("Abgebrochen.")
        return

    print("\nStarte Konvertierung...\n")

    changed_files, skipped_files = process_directory(app_dir)

    print("\n" + "=" * 60)
    print("Zusammenfassung:")
    print("=" * 60)
    print(f"Geänderte Dateien: {len(changed_files)}")
    print(f"Übersprungene Dateien: {len(skipped_files)}")

    if changed_files:
        print("\n✅ Geänderte Dateien:")
        for f in changed_files:
            print(f"  - {f.relative_to(app_dir)}")
        print(f"\n💾 Backups wurden erstellt (.bak Dateien)")

    print("\n✨ Fertig! Jetzt können Sie alle Dateien direkt debuggen.")


if __name__ == "__main__":
    main()