#!/usr/bin/env python3
"""
Script zum Wiederherstellen der Original-Dateien aus den Backups.
Speichern als: /home/ubuntu/gallery/app/rollback_imports.py
Ausführen mit: python rollback_imports.py
"""

from pathlib import Path
import shutil


def rollback_backups(directory):
    """Stellt alle .bak Dateien wieder her."""

    app_dir = Path(directory)
    restored_files = []

    # Finde alle .bak Dateien
    for bak_file in app_dir.rglob('*.bak'):
        # Überspringe __pycache__ Verzeichnisse
        if '__pycache__' in str(bak_file):
            continue

        # Originalname ohne .bak
        original_file = bak_file.with_suffix('')

        print(f"Stelle wieder her: {original_file.relative_to(app_dir)}")

        try:
            # Kopiere .bak zurück zur Original-Datei
            shutil.copy2(bak_file, original_file)

            # Lösche .bak Datei
            bak_file.unlink()

            restored_files.append(original_file)
            print(f"  ✅ Wiederhergestellt")

        except Exception as e:
            print(f"  ❌ Fehler: {e}")

    return restored_files


def main():
    """Hauptfunktion"""

    app_dir = Path(__file__).parent

    print("=" * 60)
    print("Rollback: Stelle Original-Dateien wieder her")
    print("=" * 60)
    print(f"Verzeichnis: {app_dir}")
    print()

    # Bestätigung
    response = input("Möchten Sie fortfahren? (j/n): ")
    if response.lower() not in ['j', 'ja', 'y', 'yes']:
        print("Abgebrochen.")
        return

    print("\nStarte Rollback...\n")

    restored_files = rollback_backups(app_dir)

    print("\n" + "=" * 60)
    print("Zusammenfassung:")
    print("=" * 60)
    print(f"Wiederhergestellte Dateien: {len(restored_files)}")

    if restored_files:
        print("\n✅ Wiederhergestellte Dateien:")
        for f in restored_files:
            print(f"  - {f.relative_to(app_dir)}")
        print(f"\n🗑️  Backup-Dateien (.bak) wurden gelöscht")
    else:
        print("\n⚠️  Keine Backup-Dateien gefunden")

    print("\n✨ Fertig! Alle Änderungen wurden rückgängig gemacht.")


if __name__ == "__main__":
    main()