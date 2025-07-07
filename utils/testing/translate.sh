curl http://localhost:11434/api/chat \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llava:latest",
    "stream": false,
    "messages": [
      {"role": "system", "content": "You are a professional translator. Only output the translation, do not provide any explanations."},
      {"role": "user",   "content": "Translate into German:\n<Translate the user-provided English text precisely into German.>"}
    ]
  }'
