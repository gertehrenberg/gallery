#!/bin/bash

ZIP_DIR="../cache/zips"
TEXT_FILE_CACHE_DIR="../cache/textfiles"
IMAGE_FILE_CACHE_DIR="../cache/imagefiles"

echo "🔄 Starte Entpacken und Sortieren..."

for zipfile in "$ZIP_DIR"/*.zip; do
    echo "📦 Entpacke: $zipfile"

    unzip -oq "$zipfile" -d temp_unzip_dir || continue

    find temp_unzip_dir -type f | while read -r file; do
        filename="$(basename "$file")"
        lowername="$(echo "$filename" | tr '[:upper:]' '[:lower:]')"
        ext="${lowername##*.}"

        if [[ "$ext" == "txt" ]]; then
            echo "📝 -> $TEXT_FILE_CACHE_DIR/$lowername"
            mv -n "$file" "$TEXT_FILE_CACHE_DIR/$lowername"
        else
            echo "🖼️  -> $IMAGE_FILE_CACHE_DIR/$lowername"
            mv -n "$file" "$IMAGE_FILE_CACHE_DIR/$lowername"
        fi
    done

    rm -rf temp_unzip_dir
    echo "✅ Fertig mit: $zipfile"
done

echo "🎉 Alle ZIP-Dateien verarbeitet!"