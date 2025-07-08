#!/usr/bin/env bash
set -euo pipefail

### COMPILATION ###
echo "🔨 Compilation du code Java…"
rm -rf bin
mkdir -p bin
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

# Détecte automatiquement le FQCN du Worker
if [ -f "$BIN_DIR/WorkerMultiNodes.class" ]; then
  WORKER_CLASS="WorkerMultiNodes"
elif [ -f "$BIN_DIR/mapreduce/WorkerMultiNodes.class" ]; then
  WORKER_CLASS="mapreduce.WorkerMultiNodes"
else
  echo "Erreur : WorkerMultiNodes.class introuvable dans $BIN_DIR"
  exit 1
fi

# Déploiement et lancement des Workers
echo "📦 Déploiement sur Workers…"
for wp in "${WORKERS[@]}"; do
  IFS=':' read -r HOST PORT <<< "$wp"
  echo "→ $HOST (shuffle port $PORT)"

  ssh "$HOST" "mkdir -p $REMOTE_DIR"
  scp -r "$BIN_DIR" "$WORKERS_FILE" "$HOST":"$REMOTE_DIR"/

  ssh "$HOST" bash -lc "\
    cd $REMOTE_DIR && \
    nohup java -cp $BIN_DIR $WORKER_CLASS $PORT $MASTER_IP \
      > worker-$PORT.log 2>&1 & \
    echo \"  [OK] $WORKER_CLASS lancé sur $HOST:$PORT\"\
  "
done

# Détecte automatiquement le FQCN du Master
if [ -f "$BIN_DIR/MasterMultiNodes.class" ]; then
  MASTER_CLASS="MasterMultiNodes"
elif [ -f "$BIN_DIR/mapreduce/MasterMultiNodes.class" ]; then
  MASTER_CLASS="mapreduce.MasterMultiNodes"
else
  echo "Erreur : MasterMultiNodes.class introuvable dans $BIN_DIR"
  exit 1
fi

# Lancement du Master en local
echo
echo "▶️  Démarrage du Master ($MASTER_CLASS)…"
nohup java -cp "$BIN_DIR" "$MASTER_CLASS" "$WET_FILE" "$MASTER_IP" \
  > master.log 2>&1 &

echo "  [OK] $MASTER_CLASS lancé sur port 5000"
echo
echo "✅ Déploiement terminé."
echo "   Logs Workers : worker-*.log sur chaque machine"
echo "   Log Master   : master.log en local"