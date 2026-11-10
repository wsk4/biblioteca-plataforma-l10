#!/usr/bin/env bash
# levantar.sh - un comando que levanta todo el sistema de la biblioteca: Kafka, RabbitMQ, Postgres y los
# siete servicios, todo en contenedores, con los consumer groups de biblioteca-eventos y el panel en vivo
# (L10). Supone que los ocho repositorios estan clonados uno al lado del otro, sin el sufijo -l10:
#   carpeta/
#     biblioteca-plataforma/   <- aqui va este script
#     biblioteca-prestamos/, biblioteca-eventos/, biblioteca-gateway/, ...
# Uso, en bash (Git Bash en Windows):  bash levantar.sh
set -euo pipefail
cd "$(dirname "$0")"
RAIZ="$(cd .. && pwd)"

# 1 - Los .env desde los .env.example, solo donde no existen. Son los seis que lee el compose.
REPOS="biblioteca-plataforma biblioteca-gateway biblioteca-bff biblioteca-prestamos biblioteca-eventos biblioteca-admin"
for repo in $REPOS; do
  if [ ! -f "$RAIZ/$repo/.env" ]; then
    cp "$RAIZ/$repo/.env.example" "$RAIZ/$repo/.env"
    echo "creado $repo/.env desde .env.example"
  fi
done
# Con un valor en blanco el servicio arranca mal y el error aparece lejos de la causa: se corta aqui.
faltan=0
for repo in $REPOS; do
  for nombre in $(grep -E '^[A-Za-z_][A-Za-z0-9_]*=[[:space:]]*$' "$RAIZ/$repo/.env" | cut -d= -f1); do
    echo "FALTA  $repo/.env: $nombre"
    faltan=1
  done
done
if [ "$faltan" -eq 1 ]; then
  echo "Rellena esos valores y vuelve a correr: bash levantar.sh"
  exit 1
fi

# 2 - El cluster de Kafka, esperando a que los brokers esten healthy, y sus topics (no falla si ya existen).
#     Van antes que los servicios: eventos lee los topics al arrancar y el cluster no crea topics al vuelo.
docker compose up -d --wait broker-1 broker-2 broker-3
sh kafka/topics.sh > /dev/null
echo "topics listos: biblioteca.prestamos, biblioteca.lecturas, sus .dlt y biblioteca.catalogo (compactado)"

# 3 - Todo lo demas: construye las imagenes y espera a que cada servicio quede healthy.
docker compose up -d --build --wait

# 4 - Como se comprueba que quedo arriba: cada servicio responde. Los que exigen token responden 401,
#     y un 401 es un servicio vivo.
fallas=0
comprobar() {
  esperado=$1
  url=$2
  codigo=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url" || true)
  if [ "$codigo" = "$esperado" ]; then
    echo "OK     $codigo $url"
  else
    echo "FALLA  $codigo (se esperaba $esperado) $url"
    fallas=1
  fi
}
comprobar 200 http://localhost:4200/
comprobar 401 http://localhost:8080/v1/libros
comprobar 401 http://localhost:8080/v1/panel/en-vivo
comprobar 401 http://localhost:3000/panel
comprobar 200 http://localhost:3001/libros
comprobar 200 http://localhost:3002/prestamos
comprobar 200 http://localhost:3010/salud
comprobar 200 http://localhost:3010/panel/en-vivo
comprobar 401 http://localhost:3020/v1/admin/colas
comprobar 200 http://localhost:8085/actuator/health

docker compose ps --format "table {{.Name}}\t{{.Status}}"
echo "Kafka UI: http://localhost:8085 (tiene que mostrar 3 brokers)"
echo "Panel en vivo (solo bibliotecarios): http://localhost:4200/panel"
echo "Para publicar la noche de la biblioteca: cd ../biblioteca-simulador && npm ci && node reproducir.mjs --velocidad 60"
echo "Para bajarlo todo sin borrar datos: docker compose down"
if [ "$fallas" -eq 1 ]; then
  echo "Algun servicio no responde: docker compose logs <servicio> --tail 30"
  exit 1
fi
