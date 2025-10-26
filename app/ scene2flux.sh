#!/bin/bash
# scene2flux.sh — Generate cinematic Flux prompt from scene text using Ollama

# Standardmodell (kannst du nach Bedarf ändern)
MODEL="scene2flux_13b"

# Wenn ein Modellname als erstes Argument angegeben wurde:
if [[ "$1" =~ ^[a-zA-Z0-9_-]+$ && "$2" != "" ]]; then
  MODEL="$1"
  shift
fi

# Szene zusammensetzen (alle restlichen Argumente)
SCENE="$*"

if [ -z "$SCENE" ]; then
  echo "Usage: $0 [model] \"Scene description...\""
  exit 1
fi

PROMPT="You are a visual scene composer for cinematic text-to-image generation.
Never write stories, dialogues, or inner thoughts.
Describe only what is visible in one cinematic frame.
Focus on lighting, composition, perspective, depth, materials, and atmosphere.

If the narration is first-person ('I', 'me', 'my') or implies the narrator’s presence, include her in the image:
A 20-year-old woman with a slender yet curvy figure, pale flawless skin, shoulder-length brown hair, deep purple eyes with a mysterious glow,
wearing tight black gothic clothing that accentuates her curves, dramatic dark makeup with heavy eyeliner and eyeshadow,
a small nose piercing, a tongue piercing, a tattoo on the inside of her arm, a cold and distant expression,
black gothic accessories and shoes. The character embodies dark gothic elegance with cinematic lighting and realistic texture.

Scene: $SCENE

Output a single English line describing only the visible scene with cinematic realism and append:
--width 1024 --height 1024 --steps 40 --cfg 7.5 --sampler dpmpp_2m_sde --style cinematic --quality ultra --resolution 8k --detail photorealistic --lighting volumetric --depth realistic --contrast balanced --sharpness high
"

ollama run "$MODEL" "$PROMPT"
