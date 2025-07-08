#!/usr/bin/env bash
set -euo pipefail

### CONFIGURATION ###

# Dossier contenant vos .class compilés
BIN_DIR="bin"

# workers.txt à la racine
WORKERS_FILE="workers.txt"

# Chemin vers le fichier .wet à traiter (local à la machine Master)
WET_FILE="texts/CC-MAIN-20230321002050-20230321032050-00472.warc.wet"

# IP ou hostname de la machine Master (celle où tourne ce script)
MASTER_IP="137.194.140.144"

# Répertoire distant sur les Workers (où copier bin/ + workers.txt)
REMOTE_DIR="~/mapreduce_deploy"

# Liste des Workers au format host:listenPort
# **IMPORTANT** : chaque Worker doit écouter son propre port (pas 5000 qui est le port Master)
WORKERS=(
  "tp-1a226-02:6001"
  "tp-1a226-03:6002"
  "tp-1a226-04:6003"
)

### FIN CONFIGURATION ###

echo "📦 Déploiement sur Workers..."
for wp in "${WORKERS[@]}"; do
  IFS=':' read -r HOST PORT <<< "$wp"
  echo "→ $HOST (shuffle port $PORT)"

  # Prépare le répertoire distant
  ssh "$HOST" "mkdir -p $REMOTE_DIR"

  # Copie bin/ et workers.txt
  scp -r "$BIN_DIR" "$WORKERS_FILE" "$HOST":"$REMOTE_DIR"/

  # Lance le Worker en arrière-plan, avec une seule chaîne passée à bash -lc
  ssh "$HOST" bash -lc '
    cd '"$REMOTE_DIR"' &&
    nohup java -cp '"$BIN_DIR"' WorkerMultiNodes '"$PORT"' '"$MASTER_IP"' \
      > worker-'"$PORT"'.log 2>&1 &
    echo "  [OK] WorkerMultiNodes lancé sur '"$HOST"':'"$PORT"'"'
'
done

echo
echo "▶️  Démarrage du Master en local..."

# Vérifie que bin/ et workers.txt existent en local
if [ ! -d "$BIN_DIR" ]; then
  echo "Erreur : dossier $BIN_DIR introuvable dans $(pwd)"
  exit 1
fi
if [ ! -f "$WORKERS_FILE" ]; then
  echo "Erreur : $WORKERS_FILE introuvable dans $(pwd)"
  exit 1
fi

# Lance le Master (chemin .wet et IP passés en args)
nohup java -cp "$BIN_DIR" MasterMultiNodes "$WET_FILE" "$MASTER_IP" \
  > master.log 2>&1 &

echo "  [OK] MasterMultiNodes lancé sur port 5000"
echo
echo "✅ Déploiement terminé."
echo "   Logs Workers : worker-*.log sur chaque machine"
echo "   Log Master  : master.log en local"