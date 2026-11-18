#!/usr/bin/env bash
set -Eeuo pipefail

# =====================================================
# Kafka KRaft Cluster - Rocky Linux 10
# 3 Controllers + 3 Brokers
# =====================================================

PROJECT_DIR="/opt/kafka-lab"
KAFKA_IMAGE="apache/kafka:latest"
DEFAULT_IP="192.168.1.100"

log() {
    echo
    echo "[$(date '+%F %T')] $*"
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

[[ $EUID -eq 0 ]] || die "Uruchom: sudo bash $0"

source /etc/os-release

[[ "$ID" == "rocky" && "${VERSION_ID%%.*}" == "10" ]] ||
    die "Wymagany Rocky Linux 10"

read -r -p "Adres IP serwera [${DEFAULT_IP}]: " SERVER_IP
SERVER_IP="${SERVER_IP:-$DEFAULT_IP}"

[[ "$SERVER_IP" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] ||
    die "Nieprawidłowy adres IP"

[[ ! -e "$PROJECT_DIR/compose.yaml" ]] ||
    die "Projekt już istnieje: $PROJECT_DIR"

# -----------------------------------------------------
# 1. Docker
# -----------------------------------------------------

log "Instalacja Dockera"

dnf -y install dnf-plugins-core curl ca-certificates

dnf config-manager --add-repo \
https://download.docker.com/linux/rocky/docker-ce.repo

dnf -y install \
docker-ce \
docker-ce-cli \
containerd.io \
docker-buildx-plugin \
docker-compose-plugin

systemctl enable --now docker

docker version
docker compose version

# -----------------------------------------------------
# 2. Obraz Kafka
# -----------------------------------------------------

log "Pobieranie obrazu Kafka"

docker pull "$KAFKA_IMAGE"

CLUSTER_ID=$(
docker run --rm \
--entrypoint /opt/kafka/bin/kafka-storage.sh \
"$KAFKA_IMAGE" random-uuid
)

# -----------------------------------------------------
# 3. Katalogi
# -----------------------------------------------------

log "Tworzenie katalogów"

mkdir -p "$PROJECT_DIR/config"

cd "$PROJECT_DIR"

cat > .env <<EOF
KAFKA_IMAGE=${KAFKA_IMAGE}
CLUSTER_ID=${CLUSTER_ID}
EOF

chmod 600 .env

# -----------------------------------------------------
# 4. Konfiguracja KRaft
# -----------------------------------------------------

VOTERS="1@controller-1:9093,2@controller-2:9093,3@controller-3:9093"

for N in 1 2 3
do

cat > "config/controller-${N}.properties" <<EOF
process.roles=controller
node.id=${N}

controller.quorum.voters=${VOTERS}

listeners=CONTROLLER://:9093

listener.security.protocol.map=CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER

log.dirs=/var/lib/kafka/data
EOF

done

# -----------------------------------------------------
# 5. Konfiguracja brokerów
# -----------------------------------------------------

for N in 1 2 3
do

NODE_ID=$((N + 3))
PORT=$((19091 + N))

cat > "config/broker-${N}.properties" <<EOF
process.roles=broker
node.id=${NODE_ID}

controller.quorum.voters=${VOTERS}

listeners=INTERNAL://:9092,EXTERNAL://:9094

advertised.listeners=INTERNAL://broker-${N}:9092,EXTERNAL://${SERVER_IP}:${PORT}

listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT

controller.listener.names=CONTROLLER

inter.broker.listener.name=INTERNAL

log.dirs=/var/lib/kafka/data

num.partitions=3

default.replication.factor=3
min.insync.replicas=2

offsets.topic.replication.factor=3

transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

group.initial.rebalance.delay.ms=0

auto.create.topics.enable=false
EOF

done

# -----------------------------------------------------
# 6. Docker Compose
# -----------------------------------------------------

log "Tworzenie Docker Compose"

cat > compose.yaml <<'EOF'
name: kafka-lab

services:

  controller-1:
    image: ${KAFKA_IMAGE}
    hostname: controller-1
    container_name: controller-1
    restart: unless-stopped
    environment:
      CLUSTER_ID: ${CLUSTER_ID}
    volumes:
      - ./config/controller-1.properties:/mnt/shared/config/server.properties:ro
      - controller1-data:/var/lib/kafka/data
    command:
      - /opt/kafka/bin/kafka-server-start.sh
      - /mnt/shared/config/server.properties
    networks:
      - kafka-net

  controller-2:
    image: ${KAFKA_IMAGE}
    hostname: controller-2
    container_name: controller-2
    restart: unless-stopped
    environment:
      CLUSTER_ID: ${CLUSTER_ID}
    volumes:
      - ./config/controller-2.properties:/mnt/shared/config/server.properties:ro
      - controller2-data:/var/lib/kafka/data
    command:
      - /opt/kafka/bin/kafka-server-start.sh
      - /mnt/shared/config/server.properties
    networks:
      - kafka-net

  controller-3:
    image: ${KAFKA_IMAGE}
    hostname: controller-3
    container_name: controller-3
    restart: unless-stopped
    environment:
      CLUSTER_ID: ${CLUSTER_ID}
    volumes:
      - ./config/controller-3.properties:/mnt/shared/config/server.properties:ro
      - controller3-data:/var/lib/kafka/data
    command:
      - /opt/kafka/bin/kafka-server-start.sh
      - /mnt/shared/config/server.properties
    networks:
      - kafka-net

  broker-1:
    image: ${KAFKA_IMAGE}
    hostname: broker-1
    container_name: broker-1
    restart: unless-stopped
    ports:
      - "19092:9094"
    environment:
      CLUSTER_ID: ${CLUSTER_ID}
    volumes:
      - ./config/broker-1.properties:/mnt/shared/config/server.properties:ro
      - broker1-data:/var/lib/kafka/data
    command:
      - /opt/kafka/bin/kafka-server-start.sh
      - /mnt/shared/config/server.properties
    networks:
      - kafka-net

  broker-2:
    image: ${KAFKA_IMAGE}
    hostname: broker-2
    container_name: broker-2
    restart: unless-stopped
    ports:
      - "19093:9094"
    environment:
      CLUSTER_ID: ${CLUSTER_ID}
    volumes:
      - ./config/broker-2.properties:/mnt/shared/config/server.properties:ro
      - broker2-data:/var/lib/kafka/data
    command:
      - /opt/kafka/bin/kafka-server-start.sh
      - /mnt/shared/config/server.properties
    networks:
      - kafka-net

  broker-3:
    image: ${KAFKA_IMAGE}
    hostname: broker-3
    container_name: broker-3
    restart: unless-stopped
    ports:
      - "19094:9094"
    environment:
      CLUSTER_ID: ${CLUSTER_ID}
    volumes:
      - ./config/broker-3.properties:/mnt/shared/config/server.properties:ro
      - broker3-data:/var/lib/kafka/data
    command:
      - /opt/kafka/bin/kafka-server-start.sh
      - /mnt/shared/config/server.properties
    networks:
      - kafka-net

networks:
  kafka-net:
    name: kafka-lab-net

volumes:
  controller1-data:
  controller2-data:
  controller3-data:
  broker1-data:
  broker2-data:
  broker3-data:
EOF

# -----------------------------------------------------
# 7. Skrypty administracyjne
# -----------------------------------------------------

cat > start.sh <<'EOF'
#!/bin/bash
set -e
cd /opt/kafka-lab
docker compose up -d
EOF

cat > stop.sh <<'EOF'
#!/bin/bash
set -e
cd /opt/kafka-lab
docker compose stop
EOF

cat > status.sh <<'EOF'
#!/bin/bash
cd /opt/kafka-lab
docker compose ps
EOF

cat > logs.sh <<'EOF'
#!/bin/bash
cd /opt/kafka-lab
docker compose logs -f --tail=100 "${1:-}"
EOF

cat > healthcheck.sh <<'EOF'
#!/bin/bash
set -e
cd /opt/kafka-lab

echo "=== Docker ==="
docker compose ps

echo
echo "=== KRaft QUORUM ==="

docker exec broker-1 \
/opt/kafka/bin/kafka-metadata-quorum.sh \
--bootstrap-server localhost:9092 describe --status

echo
echo "=== BROKERS ==="

docker exec broker-1 \
/opt/kafka/bin/kafka-broker-api-versions.sh \
--bootstrap-server localhost:9092
EOF

cat > create-test-topic.sh <<'EOF'
#!/bin/bash
set -e
docker exec broker-1 \
/opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create --if-not-exists \
--topic LAB.TEST \
--partitions 3 \
--replication-factor 3 \
--config min.insync.replicas=2
EOF

cat > uninstall.sh <<'EOF'
#!/bin/bash
set -e
cd /opt/kafka-lab

echo "Usunięcie kontenerów i sieci."
echo "Wolumeny danych pozostaną zachowane."
read -r -p "Kontynuować? [y/N]: " ANSWER

[[ "$ANSWER" == "y" ]] || exit 0

docker compose down

echo "Gotowe. Dane pozostały w wolumenach."
echo "Usunięcie danych wymaga: docker compose down -v"
EOF

chmod +x ./*.sh

# -----------------------------------------------------
# 8. Uruchomienie
# -----------------------------------------------------

log "Sprawdzenie konfiguracji Compose"

docker compose config -q

log "Uruchamianie klastra"

docker compose up -d

echo
echo "=========================================="
echo " Kafka KRaft Cluster został uruchomiony"
echo "=========================================="
echo
echo "Kontenery:"
docker compose ps

echo
echo "Adresy bootstrap:"
echo "${SERVER_IP}:19092"
echo "${SERVER_IP}:19093"
echo "${SERVER_IP}:19094"

echo
echo "Katalog: ${PROJECT_DIR}"
echo
echo "Komendy:"
echo "./status.sh"
echo "./logs.sh"
echo "./healthcheck.sh"
echo "./create-test-topic.sh"
echo "./stop.sh"
echo
echo "Firewall skonfiguruj osobno dla zaufanej sieci LAN."

