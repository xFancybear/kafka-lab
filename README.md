# Apache Kafka – Cluster KRaft Lab

Laboratoryjny klaster Apache Kafka uruchamiany w kontenerach Docker Compose na systemie **Rocky Linux 10**.

Projekt przedstawia konfigurację klastra Kafka wykorzystującego architekturę KRaft (Kafka Raft Metadata Mode), bez zewnętrznego Apache ZooKeeper.

Celem projektu jest zbudowanie i przetestowanie klastra składającego się z trzech dedykowanych kontrolerów oraz trzech brokerów, z replikacją danych i odpornością na awarię pojedynczego węzła.

---

## 1. Architektura klastra

Klaster składa się z sześciu kontenerów:

| Kontener | Node ID | Rola |
|---|---:|---|
| controller-1 | 1 | KRaft Controller |
| controller-2 | 2 | KRaft Controller |
| controller-3 | 3 | KRaft Controller |
| broker-1 | 4 | Kafka Broker |
| broker-2 | 5 | Kafka Broker |
| broker-3 | 6 | Kafka Broker |

### Schemat architektury

```text
                 KRaft Controller Quorum
              (3 kontrolery, większość 2/3)

              +----------------------+
              |                      |
       +-------------+       +-------------+
       | controller-1|       | controller-2|
       |   Node ID 1 |       |   Node ID 2 |
       +-------------+       +-------------+
              |                      |
              |       +-------------+
              +-------| controller-3|
                      |   Node ID 3 |
                      +-------------+

                Kafka Brokers

       +-------------+       +-------------+
       |   broker-1  |       |   broker-2  |
       |   Node ID 4 |       |   Node ID 5 |
       +-------------+       +-------------+
              \                    /
               \                  /
                +----------------+
                        |
                  +-------------+
                  |   broker-3  |
                  |   Node ID 6 |
                  +-------------+
```

Wszystkie kontenery działają na jednym hoście fizycznym. Jest to środowisko laboratoryjne, a nie konfiguracja zapewniająca odporność na awarię całego serwera.

### KRaft quorum

Kontrolery korzystają ze statycznej konfiguracji voterów:

```properties
controller.quorum.voters=1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
```

Quorum składa się z trzech kontrolerów. Do utrzymania quorum wymagane są minimum dwa działające kontrolery.

Awaria jednego kontrolera nie powinna zatrzymać pracy quorum.

---

## 2. Parametry klastra

| Parametr | Wartość |
|---|---|
| System operacyjny | Rocky Linux 10 |
| Konteneryzacja | Docker Engine |
| Orkiestracja | Docker Compose |
| Obraz Kafka | apache/kafka:latest |
| Tryb metadanych | KRaft |
| Liczba kontrolerów | 3 |
| Liczba brokerów | 3 |
| Liczba węzłów | 6 |
| Controller quorum | 3 |
| Replication Factor | 3 |
| min.insync.replicas | 2 |
| Inter-broker protocol | PLAINTEXT |
| ZooKeeper | Nie jest używany |

---

## 3. Porty i komunikacja

### Porty kontrolerów

| Kontroler | Port wewnętrzny |
|---|---:|
| controller-1 | 9093 |
| controller-2 | 9093 |
| controller-3 | 9093 |

Kontrolery nie wystawiają portów na hosta.

### Porty brokerów

| Broker | Port wewnętrzny | Port hosta |
|---|---:|---:|
| broker-1 | 9092 / 9094 | 19092 |
| broker-2 | 9092 / 9094 | 19093 |
| broker-3 | 9092 / 9094 | 19094 |

### Listeners

Brokerzy wykorzystują dwa listenery:

- `INTERNAL` – komunikacja wewnętrzna pomiędzy brokerami i kontenerami.
- `EXTERNAL` – dostęp do klastra z zewnętrznych klientów.

Przykładowa konfiguracja:

```properties
listeners=INTERNAL://:9092,EXTERNAL://:9094
advertised.listeners=INTERNAL://broker-1:9092,EXTERNAL://192.168.1.100:19092
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
```

Adres `192.168.1.100` należy dostosować do adresu IP hosta.

---

## 4. Replikacja danych

Klaster wykorzystuje:

```properties
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

Oznacza to, że:

- każda partycja może posiadać trzy repliki;
- zapis wymaga co najmniej dwóch zsynchronizowanych replik;
- awaria jednego brokera nie powinna uniemożliwić zapisu;
- utrata dwóch brokerów może spowodować brak możliwości zapisu przy wymaganiu dwóch replik ISR.

---

## 5. Instalacja

### Wymagania

- Rocky Linux 10
- Uprawnienia administratora (`root` lub `sudo`)
- Dostęp do Internetu podczas instalacji
- Dostępne porty 19092–19094
- Odpowiednia ilość pamięci RAM i przestrzeni dyskowej

### Uruchomienie instalatora

Pobierz repozytorium lub skopiuj skrypt instalacyjny na serwer.

Nadaj uprawnienia do wykonania:

```bash
chmod +x install_kafka_rocky10.sh
```

Uruchom instalator:

```bash
sudo ./install_kafka_rocky10.sh
```

Instalator przygotowuje środowisko, instaluje Docker i Docker Compose, pobiera obraz Kafka oraz generuje konfigurację klastra.

Domyślny katalog projektu:

```text
/opt/kafka-lab
```

---

## 6. Zarządzanie klastrem

Przejdź do katalogu:

```bash
cd /opt/kafka-lab
```

Dostępne skrypty:

| Skrypt | Opis |
|---|---|
| `start.sh` | Uruchomienie klastra |
| `stop.sh` | Zatrzymanie klastra |
| `status.sh` | Status kontenerów |
| `logs.sh` | Podgląd logów |
| `healthcheck.sh` | Podstawowa kontrola stanu klastra |
| `create-test-topic.sh` | Utworzenie testowego topicu |
| `uninstall.sh` | Odinstalowanie środowiska |

Przykładowe użycie:

```bash
./start.sh
./status.sh
./healthcheck.sh
```

Podgląd logów:

```bash
./logs.sh
```

---

## 7. Weryfikacja działania klastra

### 7.1. Sprawdzenie kontenerów

```bash
docker compose ps
```

Wszystkie sześć kontenerów powinno mieć status `Up`.

### 7.2. Sprawdzenie quorum KRaft

```bash
docker exec broker-1 \
/opt/kafka/bin/kafka-metadata-quorum.sh \
--bootstrap-server localhost:9092 \
describe --status
```

Najważniejsze pola:

- `LeaderId` – identyfikator lidera quorum.
- `CurrentVoters` – lista kontrolerów.
- `MaxFollowerLag` – opóźnienie replikacji metadanych.

### 7.3. Sprawdzenie brokerów

```bash
docker exec broker-1 \
/opt/kafka/bin/kafka-broker-api-versions.sh \
--bootstrap-server localhost:9092
```

Polecenie pozwala sprawdzić odpowiedzi brokerów na zapytania API Kafka.

### 7.4. Utworzenie testowego topicu

```bash
./create-test-topic.sh
```

Przykładowe parametry testowego topicu:

```text
Topic: LAB.TEST
PartitionCount: 3
ReplicationFactor: 3
min.insync.replicas=2
```

### 7.5. Sprawdzenie replikacji

```bash
docker exec broker-1 \
/opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic LAB.TEST
```

Oczekiwany wynik:

```text
Topic: LAB.TEST
PartitionCount: 3
ReplicationFactor: 3
Configs: min.insync.replicas=2

Partition: 0  Leader: 5  Replicas: 5,6,4  Isr: 5,6,4
Partition: 1  Leader: 6  Replicas: 6,4,5  Isr: 6,4,5
Partition: 2  Leader: 4  Replicas: 4,5,6  Isr: 4,5,6
```

`ISR` (In-Sync Replicas) oznacza repliki zsynchronizowane z liderem partycji.

Wszystkie repliki powinny znajdować się na liście ISR.

---

## 8. Test PRODUCE / CONSUME

### Producent

```bash
docker exec -it broker-1 \
/opt/kafka/bin/kafka-console-producer.sh \
--bootstrap-server localhost:9092 \
--topic LAB.TEST
```

Przykładowe wiadomości:

```text
wiadomosc-1
wiadomosc-2
wiadomosc-3
```

Zakończenie pracy producenta:

```text
Ctrl+C
```

### Konsument

```bash
docker exec -it broker-2 \
/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic LAB.TEST \
--from-beginning
```

Konsument powinien odczytać wysłane wcześniej wiadomości.

---

## 9. Test odporności na awarię kontrolera

Zatrzymanie jednego kontrolera:

```bash
docker stop controller-1
```

Sprawdzenie quorum:

```bash
docker exec broker-1 \
/opt/kafka/bin/kafka-metadata-quorum.sh \
--bootstrap-server localhost:9092 \
describe --status
```

Quorum powinno nadal działać dzięki pozostałym dwóm kontrolerom.

Przywrócenie kontrolera:

```bash
docker start controller-1
```

---

## 10. Test odporności na awarię brokera

Zatrzymanie jednego brokera:

```bash
docker stop broker-1
```

Sprawdzenie stanu topicu przez drugiego brokera:

```bash
docker exec broker-2 \
/opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--describe \
--topic LAB.TEST
```

Oczekiwane zachowanie:

- pozostałe dwa brokery nadal działają;
- partycje mają dostępne repliki;
- ISR może zmniejszyć się z trzech do dwóch;
- zapis powinien być możliwy, jeśli co najmniej dwie repliki pozostają w ISR.

Przywrócenie brokera:

```bash
docker start broker-1
```

Po ponownym dołączeniu brokera należy sprawdzić, czy wszystkie repliki wróciły do ISR.

---

## 11. Dostęp z sieci LAN

Klienci zewnętrzni mogą łączyć się z brokerami za pomocą adresu IP hosta.

Przykładowe adresy bootstrap:

```text
192.168.1.100:19092
192.168.1.100:19093
192.168.1.100:19094
```

Test połączenia TCP z innego komputera:

```bash
nc -vz 192.168.1.100 19092
nc -vz 192.168.1.100 19093
nc -vz 192.168.1.100 19094
```

W razie potrzeby należy skonfigurować odpowiednie reguły zapory systemowej.

---

## 12. Bezpieczeństwo

Projekt wykorzystuje protokół `PLAINTEXT`.

Oznacza to brak:

- szyfrowania TLS;
- uwierzytelniania klientów;
- autoryzacji ACL.

Konfiguracja przeznaczona jest do środowiska laboratoryjnego i zaufanej sieci LAN.

Nie należy wystawiać brokerów bezpośrednio do Internetu.

W środowisku produkcyjnym należy skonfigurować odpowiednie mechanizmy bezpieczeństwa, uwierzytelnianie i szyfrowanie komunikacji.

---

## 13. Ważne informacje

- Wszystkie kontenery działają na jednym hoście fizycznym.
- Awaria hosta oznacza niedostępność całego klastra.
- Dane każdego węzła przechowywane są w oddzielnym wolumenie Docker.
- Nie należy usuwać wolumenów bez świadomej decyzji o usunięciu danych.
- Do długotrwałego użytkowania zalecane jest przypięcie konkretnej wersji obrazu Kafka zamiast `latest`.
- Przed wykorzystaniem produkcyjnym należy przeprowadzić dodatkowe testy wydajności, bezpieczeństwa i odtwarzania danych.

---

## 14. Cel projektu

Projekt służy do praktycznego poznania i testowania:

- architektury KRaft;
- quorum kontrolerów;
- działania brokerów Kafka;
- replikacji partycji;
- mechanizmu ISR;
- parametrów `Replication Factor` i `min.insync.replicas`;
- komunikacji wewnętrznej i zewnętrznej;
- zachowania klastra podczas awarii kontrolera i brokera.

**Technologie:** Apache Kafka · KRaft · Docker · Docker Compose · Rocky Linux 10
