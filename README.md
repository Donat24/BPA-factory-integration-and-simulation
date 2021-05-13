# BPA-factory-integration-and-simulation

## Quick Start
Auf dem System muss Docker sowie Docker-Compose installiert sein.

1. `docker-compose build`
2. `docker-compose up (-d for detached)`
## Zielstellung
Zielstellung des Projektes ist die visuelle Darstellung der Overall Equipment Effectiveness (OEE) von Maschinen durch Anwendung von Services in AWS. Dafür wurde der Kurs in drei Gruppen unterteilt, wobei jede Gruppe eine einzelne Zielstellung bearbeitet. Die einzelnen Themen sind:
1. Factory Integration & Simulation
2. Data Processing & Analytics
3. UI & external Connectors

Konkret soll Gruppe 1 Field Devices simulieren und die Nachrichten dieser zur weiterern Verarbeitung in AWS Verfügbar machen. Gruppe 2 soll diese Nachrichten verarbeiten, die zur Berechnung der OEE nötigen Werte aggregieren und in einer konsumierbaren Form für Gruppe 3 bereitstellen. Gruppe 3 hat die Aufgabe den Verlauf der OEE und gegebenenfalls weitere Kennzahlen visuell darzustellen und gleichzeitig für einen externen Zugang zu sorgen.

## Architektur und Schnittstellen
![Architecture](drawings/BPA_Architecture.png)


## Beschreibung entwickelter Artefakte und verwendeter Cloud Services
### Field Device
Die simulierte Maschine ist eine Getränkeabfüllanlage. Diese wird mithilfe eines Python Skripts simuliert. Die Containerisierung ermöglicht die einfache Verwendung der Simulation auf neuen Servern und soll gleichzeitig dafür sorgen, das mehrere Simulationen auf einem oder auf verschiedenen Servern erzeugt werden können.

Um eine sinnvolle OEE zu berechnen hat die Maschine eine Plabelegungszeit und Planmenge hinterlegt. Die Flasche sendet Informationen über die Abfüllung neuer Flaschen, der Aussortierung von Flaschen und dem Stop der Maschine. Aufgrund zufälliger Ereignisse kommt es gelegentlich zu Ausfällen oder zur Aussortierung von Flaschen, was für eine geringere Planmenge sorgt.

**Datenmodell:**

### IoT Core

### IoT Rules Engine

### DynamoDB

