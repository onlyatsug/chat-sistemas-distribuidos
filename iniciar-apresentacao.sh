#!/bin/sh
# ==============================================================================
# Script Unificado de Apresentação Ao Vivo — Chat Distribuído (Pessoas 01 a 04)
# Funciona em:
#   1. Ambiente Linux local ou VM na Nuvem (via Docker Compose)
#   2. Hospedado dentro de Container Único na Nuvem (Render, Railway, Fly.io,
#      Cloud Run, Codespaces, etc. — via Dockerfile.apresentacao ou modo container)
#
# Comandos rápidos durante a apresentação:
#   ./iniciar-apresentacao.sh              -> Sobe cluster + balanceador + painel
#   ./iniciar-apresentacao.sh escalar 6    -> Escala o cluster para 6 nós ao vivo
#   ./iniciar-apresentacao.sh derrubar     -> Derruba 1 nó (stop) p/ mostrar failover
#   ./iniciar-apresentacao.sh derrubar kill-> Mata 1 nó abruptamente (queda de luz)
#   ./iniciar-apresentacao.sh religar      -> Religa o nó derrubado
#   ./iniciar-apresentacao.sh status       -> Mostra saúde do cluster e URLs
#   ./iniciar-apresentacao.sh benchmark    -> Roda bateria comparativa de métricas
#   ./iniciar-apresentacao.sh parar        -> Desliga todos os serviços
# ==============================================================================

set -eu
cd "$(dirname "$0")"

ACAO="${1:-auto}"
ARG2="${2:-}"

NOS="${NOS:-3}"
LB_PORT="${LB_PORT:-${PORT:-4000}}"
PANEL_PORT="${PANEL_PORT:-4040}"
BIND_IP="${BIND_IP:-0.0.0.0}"
COOKIE="${COOKIE:-chat_sd_cookie}"

COMPOSE_FILES="-f compose.lb.yml -f compose.metricas.yml"

# Detecta comando docker (com ou sem sudo, comum no Linux/CachyOS/Ubuntu)
DOCKER_CMD=""
detectar_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    return 1
  fi
  if docker info >/dev/null 2>&1; then
    DOCKER_CMD="docker"
    return 0
  fi
  # Se o daemon estiver parado em um host Linux com systemd, tenta iniciá-lo
  if command -v systemctl >/dev/null 2>&1 && command -v sudo >/dev/null 2>&1; then
    if ! systemctl is-active --quiet docker 2>/dev/null; then
      echo ">> Serviço Docker está parado. Tentando iniciar com 'sudo systemctl start docker'..."
      sudo systemctl start docker || true
      sleep 2
    fi
  fi
  if docker info >/dev/null 2>&1; then
    DOCKER_CMD="docker"
    return 0
  fi
  if command -v sudo >/dev/null 2>&1 && sudo -n docker info >/dev/null 2>&1; then
    DOCKER_CMD="sudo docker"
    return 0
  fi
  if command -v sudo >/dev/null 2>&1; then
    echo ">> Permissão elevada necessária para acessar o Docker..."
    if sudo docker info >/dev/null 2>&1; then
      DOCKER_CMD="sudo docker"
      return 0
    fi
  fi
  return 1
}

obter_ip_rede() {
  if command -v hostname >/dev/null 2>&1; then
    hostname -I 2>/dev/null | awk '{print $1}' || true
  elif command -v ip >/dev/null 2>&1; then
    ip route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}' | head -n 1 || true
  fi
}

aguardar_http() {
  url="$1"
  tentativas="${2:-40}"
  i=0
  while [ "$i" -lt "$tentativas" ]; do
    if command -v curl >/dev/null 2>&1; then
      if curl -fsS "$url" >/dev/null 2>&1; then return 0; fi
    elif command -v node >/dev/null 2>&1; then
      if node -e "fetch('$url').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))" 2>/dev/null; then return 0; fi
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

imprimir_painel_urls() {
  modo_label="$1"
  ip_local=$(obter_ip_rede)
  echo ""
  echo "======================================================================"
  echo " APRESENTAÇÃO PRONTA! ($modo_label)"
  echo "======================================================================"
  echo "  Chat Distribuído (Usuários) : http://localhost:${LB_PORT}"
  if [ "$modo_label" = "Modo Container Único Nuvem" ]; then
    echo "  Painel de Métricas Ao Vivo  : http://localhost:${LB_PORT}/painel/"
    echo "  Painel (Porta Direta)       : http://localhost:${PANEL_PORT}"
  else
    echo "  Painel de Métricas Ao Vivo  : http://localhost:${PANEL_PORT}"
  fi
  echo "  Telemetria JSON (/metrics)  : http://localhost:${LB_PORT}/metrics"
  echo "  Saúde do Cluster (/health)  : http://localhost:${LB_PORT}/health"
  if [ -n "$ip_local" ] && [ "$BIND_IP" = "0.0.0.0" ]; then
    echo "----------------------------------------------------------------------"
    echo "  Acesso na Rede / Nuvem (LAN/VM):"
    echo "    Chat   -> http://${ip_local}:${LB_PORT}"
    if [ "$modo_label" = "Modo Container Único Nuvem" ]; then
      echo "    Painel -> http://${ip_local}:${LB_PORT}/painel/"
    else
      echo "    Painel -> http://${ip_local}:${PANEL_PORT}"
    fi
  fi
  if [ "$modo_label" != "Modo Container Único Nuvem" ]; then
    echo "----------------------------------------------------------------------"
    echo "  Atalhos para demonstrar ao vivo durante a fala:"
    echo "    ./iniciar-apresentacao.sh escalar 6       # Escala p/ 6 servidores"
    echo "    ./iniciar-apresentacao.sh derrubar        # Derruba 1 nó (SIGTERM)"
    echo "    ./iniciar-apresentacao.sh derrubar kill   # Queda abrupta (SIGKILL)"
    echo "    ./iniciar-apresentacao.sh religar         # Religa o nó derrubado"
    echo "    ./iniciar-apresentacao.sh status          # Inspeciona o cluster"
    echo "    ./iniciar-apresentacao.sh parar           # Encerra tudo"
  fi
  echo "======================================================================"
  echo ""
}

# ==============================================================================
# MODO 1: Container Único na Nuvem (sem daemon Docker externo)
# Sobe N nós BEAM reais em portas locais + Nginx unificado + Painel Node.js
# ==============================================================================
iniciar_modo_container() {
  echo "== [Apresentação Nuvem/Container] Inicializando cluster BEAM de $NOS nós no mesmo container..."

  if command -v epmd >/dev/null 2>&1; then
    epmd -daemon || true
  fi

  # Monta lista estática de nós BEAM locais: chat1@127.0.0.1,chat2@127.0.0.1,...
  CLUSTER_LIST=""
  UPSTREAM_SERVERS=""
  i=1
  while [ "$i" -le "$NOS" ]; do
    NODE_PORT=$((4000 + i))
    NODE_ID="chat${i}@127.0.0.1"
    if [ -z "$CLUSTER_LIST" ]; then
      CLUSTER_LIST="$NODE_ID"
    else
      CLUSTER_LIST="${CLUSTER_LIST},${NODE_ID}"
    fi
    UPSTREAM_SERVERS="${UPSTREAM_SERVERS}    server 127.0.0.1:${NODE_PORT} max_fails=1 fail_timeout=5s;
"
    i=$((i + 1))
  done

  PIDS=""
  limpar_container() {
    echo ">> Encerrando processos do container..."
    for p in $PIDS; do
      kill "$p" 2>/dev/null || true
    done
    exit 0
  }
  trap limpar_container INT TERM

  # Inicia os N nós Elixir/Phoenix
  i=1
  while [ "$i" -le "$NOS" ]; do
    NODE_PORT=$((4000 + i))
    NODE_ID="chat${i}@127.0.0.1"
    echo "   -> Subindo nó BEAM $NODE_ID na porta interna $NODE_PORT..."
    BIND_IP="127.0.0.1" \
    PORT="$NODE_PORT" \
    COOKIE="$COOKIE" \
    CLUSTER_NODES="$CLUSTER_LIST" \
    ERL_FLAGS="-name ${NODE_ID} -setcookie ${COOKIE} +S 2:2" \
    mix phx.server &
    PIDS="$PIDS $!"
    i=$((i + 1))
  done

  # Aguarda os nós internos responderem /health
  i=1
  while [ "$i" -le "$NOS" ]; do
    NODE_PORT=$((4000 + i))
    aguardar_http "http://127.0.0.1:${NODE_PORT}/health" 60
    i=$((i + 1))
  done

  # Gera configuração Nginx compatível com HTTP/HTTPS de qualquer provedor Cloud
  NGINX_CONF="/tmp/nginx-apresentacao.conf"
  cat > "$NGINX_CONF" <<EOF
worker_processes auto;
pid /tmp/nginx-apresentacao.pid;
error_log /dev/stderr warn;

events { worker_connections 4096; }

http {
  access_log off;
  client_body_temp_path /tmp/nginx_body;
  proxy_temp_path /tmp/nginx_proxy;
  fastcgi_temp_path /tmp/nginx_fastcgi;
  uwsgi_temp_path /tmp/nginx_uwsgi;
  scgi_temp_path /tmp/nginx_scgi;

  map \$http_upgrade \$connection_upgrade {
    default upgrade;
    ''      close;
  }

  upstream chat_cluster {
    least_conn;
${UPSTREAM_SERVERS}  }

  server {
    listen 0.0.0.0:${LB_PORT};

    location = /lb-status {
      stub_status;
    }

    location = /painel {
      return 302 /painel/;
    }

    location /painel/ {
      proxy_pass http://127.0.0.1:${PANEL_PORT}/painel/;
      proxy_http_version 1.1;
      proxy_set_header Host \$http_host;
      proxy_buffering off;
      proxy_cache off;
      proxy_read_timeout 3600s;
    }

    location / {
      proxy_pass http://chat_cluster;
      proxy_http_version 1.1;
      proxy_set_header Upgrade \$http_upgrade;
      proxy_set_header Connection \$connection_upgrade;
      proxy_set_header Host \$http_host;
      # Compatibilidade automática caso o container esteja atrás de HTTPS na nuvem
      proxy_set_header Origin http://\$http_host;
      proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
      proxy_connect_timeout 1s;
      proxy_next_upstream error timeout http_502 http_503;
      proxy_read_timeout 120s;
      proxy_send_timeout 120s;
      add_header X-Chat-Upstream \$upstream_addr always;
    }
  }
}
EOF

  echo "   -> Iniciando Balanceador Nginx na porta ${LB_PORT}..."
  nginx -c "$NGINX_CONF" -g "daemon off;" &
  PIDS="$PIDS $!"

  aguardar_http "http://127.0.0.1:${LB_PORT}/health" 20

  echo "   -> Iniciando Painel de Métricas Ao Vivo na porta ${PANEL_PORT} (e rota /painel/)..."
  PANEL_PORT="$PANEL_PORT" \
  PANEL_BIND="0.0.0.0" \
  TARGET_URL="http://127.0.0.1:${LB_PORT}" \
  node scripts/painel-metricas.mjs &
  PANEL_PID=$!
  PIDS="$PIDS $PANEL_PID"

  imprimir_painel_urls "Modo Container Único Nuvem"
  wait "$PANEL_PID"
}

# ==============================================================================
# MODO 2: Orquestração Completa Docker Compose (Linux Local ou VM Cloud)
# ==============================================================================
executar_compose() {
  LB_BIND="$BIND_IP" LB_PORT="$LB_PORT" PANEL_BIND="$BIND_IP" PANEL_PORT="$PANEL_PORT" \
    $DOCKER_CMD compose $COMPOSE_FILES "$@"
}

if [ "$ACAO" = "container" ]; then
  iniciar_modo_container
  exit 0
fi

if [ "$ACAO" = "auto" ] || [ "$ACAO" = "iniciar" ] || [ "$ACAO" = "up" ]; then
  if detectar_docker; then
    echo "== [Apresentação Docker/Linux] Subindo Cluster ($NOS nós BEAM) + Nginx + Painel de Métricas..."
    executar_compose up --build -d --scale chat="$NOS" --wait
    executar_compose restart painel-metricas >/dev/null 2>&1 || true

    echo ">> Aguardando convergência do cluster e painel..."
    aguardar_http "http://127.0.0.1:${LB_PORT}/metrics" 30 || true
    aguardar_http "http://127.0.0.1:${PANEL_PORT}/health" 30 || true

    imprimir_painel_urls "Cluster Docker (${NOS} nós BEAM)"
    exit 0
  elif command -v mix >/dev/null 2>&1 && command -v node >/dev/null 2>&1 && command -v nginx >/dev/null 2>&1; then
    echo ">> Daemon Docker não detectado; iniciando automaticamente em Modo Container Autônomo..."
    iniciar_modo_container
    exit 0
  else
    echo "ERRO: Docker não está em execução (ou inacessível). Inicie o Docker com 'sudo systemctl start docker'."
    exit 1
  fi
fi

detectar_docker || { echo "ERRO: Docker indisponível."; exit 1; }

case "$ACAO" in
  escalar)
    NOVO_TOTAL="${ARG2:-6}"
    echo "== Escalando o cluster ao vivo para $NOVO_TOTAL nós BEAM..."
    executar_compose up -d --scale chat="$NOVO_TOTAL" --wait
    echo ">> Pronto! Acompanhe os novos nós entrando automaticamente no Painel (:4040)."
    ;;

  derrubar)
    MODO_QUEDA="${ARG2:-stop}"
    VITIMA=$($DOCKER_CMD ps --filter label=com.docker.compose.project=chatsd --filter label=com.docker.compose.service=chat --format '{{.Names}}' | sort | sed -n 2p)
    if [ -z "$VITIMA" ]; then
      VITIMA=$($DOCKER_CMD ps --filter label=com.docker.compose.project=chatsd --filter label=com.docker.compose.service=chat --format '{{.Names}}' | head -n 1)
    fi
    [ -n "$VITIMA" ] || { echo "Nenhum container de chat ativo encontrado."; exit 1; }
    echo ">> Derrubando nó $VITIMA com 'docker $MODO_QUEDA'..."
    $DOCKER_CMD "$MODO_QUEDA" "$VITIMA"
    echo ">> Nó $VITIMA derrubado! Veja o failover automático no Chat (:4000) e no Painel (:4040)."
    ;;

  religar)
    PARADOS=$($DOCKER_CMD ps -a --filter label=com.docker.compose.project=chatsd --filter label=com.docker.compose.service=chat --filter status=exited --format '{{.Names}}')
    if [ -z "$PARADOS" ]; then
      echo ">> Nenhum nó parado para religar."
    else
      for c in $PARADOS; do
        echo ">> Religando $c..."
        $DOCKER_CMD start "$c"
      done
      echo ">> Nó(s) religado(s) e sincronizando catálogo via PubSub!"
    fi
    ;;

  status)
    echo "== Containers ativos no projeto chatsd:"
    executar_compose ps
    echo ""
    echo "== Resumo do Cluster (/health):"
    curl -fsS "http://127.0.0.1:${LB_PORT}/health" && echo "" || echo "Indisponível"
    ;;

  benchmark)
    sh scripts/benchmark-escala.sh
    ;;

  parar|down)
    echo "== Encerrando todos os containers da apresentação..."
    executar_compose down
    echo ">> Ambiente encerrado."
    ;;

  *)
    echo "Uso: ./iniciar-apresentacao.sh [iniciar | escalar <N> | derrubar [stop|kill] | religar | status | benchmark | parar | container]"
    exit 1
    ;;
esac
