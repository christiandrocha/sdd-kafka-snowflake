#!/bin/bash
# infra/scripts/set_compatibility.sh
set -euo pipefail

REGISTRY_URL="http://localhost:8081"
GREEN="\033[92m"; YELLOW="\033[93m"; RED="\033[91m"
CYAN="\033[96m"; GRAY="\033[90m"; RESET="\033[0m"
cmd="${1:-help}"

case "$cmd" in
  list)
    echo -e "\n${CYAN}── Subjects registrados ───────────────────────────${RESET}"
    curl -sf "${REGISTRY_URL}/subjects" | python3 -c "
import sys, json
subjects = json.load(sys.stdin)
for s in sorted(subjects): print(f'  {s}')
print(f'\nTotal: {len(subjects)} subject(s)')
"
    echo "" ;;

  show)
    subject="${2:-}"
    [ -z "$subject" ] && echo -e "${RED}Uso: $0 show <subject>${RESET}" && exit 1
    echo -e "\n${CYAN}── Schema: ${subject} ──────────────────────────────${RESET}"
    curl -sf "${REGISTRY_URL}/subjects/${subject}/versions/latest" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  ID      : {d[\"id\"]}')
print(f'  Versão  : {d[\"version\"]}')
schema = json.loads(d['schema'])
print(f'  Nome    : {schema.get(\"name\", \"—\")}')
print(f'\n  Campos:')
for f in schema.get('fields', []):
    tipo = f['type']
    if isinstance(tipo, list): tipo = [t for t in tipo if t != 'null']; tipo = tipo[0] if tipo else 'null'
    nullable = 'null' in f['type'] if isinstance(f['type'], list) else False
    print(f'    {f[\"name\"]:<25} {str(tipo):<20} nullable={nullable}  default={f.get(\"default\", \"—\")}')
"
    echo "" ;;

  versions)
    subject="${2:-}"
    [ -z "$subject" ] && echo -e "${RED}Uso: $0 versions <subject>${RESET}" && exit 1
    echo -e "\n${CYAN}── Versões: ${subject} ──────────────────────────────${RESET}"
    curl -sf "${REGISTRY_URL}/subjects/${subject}/versions" | python3 -c "
import sys,json; versions=json.load(sys.stdin); print(f'  Versões: {versions}')
"
    echo "" ;;

  check)
    subject="${2:-}"; schema_file="${3:-}"
    [ -z "$subject" ] || [ -z "$schema_file" ] && echo -e "${RED}Uso: $0 check <subject> <arquivo.avsc>${RESET}" && exit 1
    echo -e "\n${YELLOW}🔍  Testando compatibilidade de ${schema_file} com ${subject}...${RESET}"
    SCHEMA_JSON=$(python3 -c "import json; print(json.dumps({'schema': open('${schema_file}').read()}))")
    RESULT=$(curl -sf -X POST "${REGISTRY_URL}/compatibility/subjects/${subject}/versions/latest" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" -d "$SCHEMA_JSON")
    IS_COMPAT=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('is_compatible', False))")
    [ "$IS_COMPAT" = "True" ] \
        && echo -e "${GREEN}✅  Schema compatível (BACKWARD)${RESET}" \
        || echo -e "${RED}✖   Schema INCOMPATÍVEL${RESET}\n${GRAY}    ${RESULT}${RESET}"
    echo "" ;;

  compat)
    subject="${2:-}"
    if [ -z "$subject" ]; then
        echo -e "\n${CYAN}── Compatibilidade global ──────────────────────────${RESET}"
        curl -sf "${REGISTRY_URL}/config" | python3 -c "
import sys,json; d=json.load(sys.stdin); print(f'  Nível global: {d.get(\"compatibilityLevel\", \"—\")}')
"
    else
        echo -e "\n${CYAN}── Compatibilidade: ${subject} ──────────────────────${RESET}"
        curl -sf "${REGISTRY_URL}/config/${subject}" 2>/dev/null | python3 -c "
import sys,json; d=json.load(sys.stdin); print(f'  Nível: {d.get(\"compatibilityLevel\", \"usa global\")}')
" || echo -e "  ${GRAY}Nenhuma config específica — usa o nível global${RESET}"
    fi
    echo "" ;;

  help|*)
    echo -e "\n${CYAN}set_compatibility.sh — Gerenciamento do Schema Registry${RESET}\n"
    echo -e "  ${GREEN}list${RESET}                           Lista todos os subjects"
    echo -e "  ${GREEN}show${RESET}     <subject>             Exibe campos e tipos do schema"
    echo -e "  ${GREEN}versions${RESET} <subject>             Lista versões disponíveis"
    echo -e "  ${GREEN}check${RESET}    <subject> <arquivo>   Testa compatibilidade"
    echo -e "  ${GREEN}compat${RESET}   [subject]             Exibe nível de compatibilidade"
    echo -e "\n  ${GRAY}Subjects após pipeline iniciar:${RESET}"
    echo -e "  ${GRAY}  pg.public.usuarios-value${RESET}"
    echo -e "  ${GRAY}  pg.public.produtos-value${RESET}\n" ;;
esac
