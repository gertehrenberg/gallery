80, 8888, 5678, 11434, 8001, 8002, 8003, 8004, 8188

 nano ~/.ssh/config

ssh runpod_root
bash /workspace/setup_gert_user.sh    # legt den User an und kopiert SSH-Keys
bash /workspace/setup_ssh.sh          # (optional) weitere SSH/Einstellungen
bash /workspace/setup_n8n.sh          # installiert Node.js & n8n, migriert DB in /workspace/.n8n
exit                                  # zurück in deinen lokalen Rechner

ssh runpod

./setup_conda_path.sh
conda init bash
source ~/.bash_profile
conda deactivate
conda activate /workspace/envs/gpu-env

mkdir -p /mnt/models/{checkpoints,loras,controlnet,vae,embeddings,upscale_models,gligen}


wget \
  "https://civitai.com/api/download/models/14856?type=Model&format=SafeTensor&size=full&fp=fp16&token=YOUR_API_KEY" \
  --content-disposition




  conda clean --all --yes
  rm -rf ComfyUI/temp


