#!/usr/bin/env bash
set -euo pipefail

echo "🔨 Compilation du code Java…"
# Vide l'ancien bin, on repart sur du propre
rm -rf bin
mkdir -p bin
# Compile tous les .java sous src/main/java
find src/main/java -name '*.java' > sources.txt
javac -d bin @sources.txt
rm sources.txt

### CONFIGURATION ###
BIN_DIR="bin"
WORKERS_FILE="workers.txt"
WET_FILE="texts/CC-MAIN-20230321002050-20230321032050-00472.warc.wet"
MASTER_IP="137.194.140.144"
REMOTE_DIR="~/mapreduce_deploy"
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

  ssh "$HOST" "mkdir -p $REMOTE_DIR"
  scp -r "$BIN_DIR" "$WORKERS_FILE" "$HOST":"$REMOTE_DIR"/

  # On passe la commande entière entre doubles quotes,
  # et on échappe les guillemets intérieurs.
  ssh "$HOST" bash -lc "\
    cd $REMOTE_DIR && \
    nohup java -cp $BIN_DIR mapreduce.WorkerMultiNodes $PORT $MASTER_IP \
      > worker-$PORT.log 2>&1 & \
    echo \"  [OK] WorkerMultiNodes lancé sur $HOST:$PORT\"\
  "
done

echo
echo "▶️  Démarrage du Master en local..."
if [ ! -d "$BIN_DIR" ]; then
  echo "Erreur : dossier $BIN_DIR introuvable dans $(pwd)"
  exit 1
fi
if [ ! -f "$WORKERS_FILE" ]; then
  echo "Erreur : $WORKERS_FILE introuvable dans $(pwd)"
  exit 1
fi

nohup java -cp "$BIN_DIR" mapreduce.MasterMultiNodes "$WET_FILE" "$MASTER_IP" \
  > master.log 2>&1 &

echo "  [OK] MasterMultiNodes lancé sur port 5000"
echo
echo "✅ Déploiement terminé."
echo "   Logs Workers : worker-*.log sur chaque machine"
echo "   Log Master   : master.log en local"