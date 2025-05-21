#!/bin/bash
timestamp=$(date +"%Y-%m-%d_%H-%M")
zipfile="$HOME/backups/${timestamp}_full_backup.zip"

N8N_DIR="$HOME/n8n-docker"
NSFW_DIR="$HOME/nsfw_service"
GALLERY_DIR="$HOME/gallery"

mkdir -p "$HOME/backups"

zip -r "$zipfile" \
  "$N8N_DIR" \
  "$NSFW_DIR" \
  "$GALLERY_DIR" \
  "$GALLERY_DIR/cache/"*.json \
  -x "*.pyc" \
  -x "*.log" \
  -x "*.zip" \
  \
  -x "$N8N_DIR/caddy-data/**" \
  -x "$N8N_DIR/caddy-config/**" \
  -x "$N8N_DIR/.idea/**" \
  -x "$N8N_DIR/**/__pycache__/*" \
  -x "$N8N_DIR/**/.git*" \
  \
  -x "$GALLERY_DIR/cache/**" \
  -x "$GALLERY_DIR/temp/**" \
  -x "$GALLERY_DIR/thumbnails/**" \
  -x "$GALLERY_DIR/.idea/**" \
  -x "$GALLERY_DIR/**/__pycache__/*" \
  -x "$GALLERY_DIR/**/.git*" \
  \
  -x "$NSFW_DIR/.idea/**" \
  -x "$NSFW_DIR/**/__pycache__/*" \
  -x "$NSFW_DIR/**/.git*"
