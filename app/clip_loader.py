from pathlib import Path

from transformers import CLIPModel, CLIPProcessor

model_path = Path(__file__).resolve().parent.parent / "clip_model"

clip_model = CLIPModel.from_pretrained(
    str(model_path),
    local_files_only=True
    # ✅ Kein from_safetensors
)

clip_processor = CLIPProcessor.from_pretrained(
    str(model_path),
    local_files_only=True,
    use_fast=True
)
