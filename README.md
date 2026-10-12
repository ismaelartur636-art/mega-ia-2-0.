# MEGA IA Parceiro 2.0 — protótipo novo

Interface recriada em `index.html`; backend novo em `main.py`; único indicador reaproveitado: Scalping M1 FLEX.

## Deploy Render

- Build: `pip install -r requirements.txt` **e instale uma versão auditada e compatível de `iqoptionapi`**.
- Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Configure HTTPS no Render.
- Abra `/health` para verificar `iq_library: true`.

## Limitações relevantes

- A IQ Option não oferece aqui integração oficial garantida. Login/WebSocket podem falhar com TimeoutError, especialmente no Render; reconstruir o app não resolve restrições da rede/corretora.
- Não há execução de ordens. Resultados são virtuais, derivados de candles recebidos.
- As sessões e resultados ficam em memória: reinícios do servidor apagam o histórico; o app não está pronto para produção com múltiplos workers/usuários.
- Esta primeira versão oferece somente **recuperação no próximo sinal**; não habilita Gale automático.
- A conexão deve ser validada antes de usar sinais; não existe fallback para Yahoo nem geração artificial de candles.
- Cada cliente deve ter sua própria sessão, mas faltam licenças e persistência de banco de dados para produção.
