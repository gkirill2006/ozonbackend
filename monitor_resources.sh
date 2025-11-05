#!/bin/bash

# Скрипт мониторинга ресурсов Docker контейнеров
# Использование: ./monitor_resources.sh

echo "=== Мониторинг ресурсов Docker контейнеров ==="
echo "Время: $(date)"
echo

# Проверяем использование ресурсов контейнерами
echo "📊 Использование ресурсов контейнерами:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | head -10

echo
echo "🔍 Проверка состояния контейнеров:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo
echo "⚠️  Контейнеры с высоким потреблением CPU (>80%):"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | awk 'NR>1 && $2+0 > 80 {print $0}'

echo
echo "💾 Контейнеры с высоким потреблением памяти (>70%):"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | awk 'NR>1 && $4+0 > 70 {print $0}'

echo
echo "📋 Логи системных ошибок за последние 10 минут:"
docker logs markets_celery --since=10m 2>&1 | grep -i "error\|exception\|timeout" | tail -5
docker logs markets_db --since=10m 2>&1 | grep -i "error\|exception\|timeout" | tail -5

echo
echo "🔄 Health checks:"
docker inspect markets_backend --format='{{.State.Health.Status}}' 2>/dev/null || echo "Не настроен"
docker inspect markets_celery --format='{{.State.Health.Status}}' 2>/dev/null || echo "Не настроен"
docker inspect markets_db --format='{{.State.Health.Status}}' 2>/dev/null || echo "Не настроен"
docker inspect markets_redis --format='{{.State.Health.Status}}' 2>/dev/null || echo "Не настроен"

echo
echo "Закончено: $(date)"

