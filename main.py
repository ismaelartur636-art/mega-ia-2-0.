"""MEGA IA Parceiro 2.0 - projeto novo, motor Scalping M1 FLEX."""
import os, time, secrets, threading, logging, socket, ssl, re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

try:
    from iqoptionapi.stable_api import IQ_Option
except ImportError:
    IQ_Option = None

app = FastAPI(title="MEGA IA Parceiro", version="2.0.0")
LOG = logging.getLogger("mega_parceiro")
BR = ZoneInfo("America/Sao_Paulo")
SESSIONS = {}
LOCK = threading.RLock()
POOL = ThreadPoolExecutor(max_workers=4)
SESSION_TTL = 12 * 3600
LOGIN_TIMEOUT = 35
IQ_LOGIN_GUARD = threading.Lock()
IQ_LOGIN_STATE_LOCK = threading.Lock()
IQ_LOGIN_STATE = {"stage":"idle", "started":None, "elapsed":None, "result":"not_started", "error_code":None}
IQ_LOGIN_WORKER = {"future": None}

def iq_login_stage(stage, result="running", started=None):
    with IQ_LOGIN_STATE_LOCK:
        IQ_LOGIN_STATE.update(stage=stage, result=result)
        if started is not None:
            IQ_LOGIN_STATE["started"] = started
        if IQ_LOGIN_STATE.get("started") is not None:
            IQ_LOGIN_STATE["elapsed"] = round(time.monotonic()-IQ_LOGIN_STATE["started"], 1)
    LOG.info("[IQ CONNECTOR] etapa=%s resultado=%s", stage, result)

@app.get("/iq-diagnostics")
def iq_diagnostics():
    with IQ_LOGIN_STATE_LOCK:
        info = dict(IQ_LOGIN_STATE)
    started = info.pop("started", None)
    if started is not None and info.get("result") in ("running", "timed_out_worker_may_still_be_running"):
        info["elapsed"] = round(time.monotonic() - started, 1)
    worker = IQ_LOGIN_WORKER.get("future")
    return {"iq_library": IQ_Option is not None, "login": info,
            "login_busy": IQ_LOGIN_GUARD.locked(),
            "worker_running": bool(worker and not worker.done()),
            "timeout_seconds": LOGIN_TIMEOUT}

@app.get("/iq-network-test")
def iq_network_test():
    """Diagnóstico sem credenciais do host e WebSocket usados pela biblioteca."""
    from urllib.parse import urlsplit
    # A biblioteca comunitária usa normalmente iqoption.com/echo/websocket.
    # A URL efetiva pode ser diferente em outras versões/forks instalados.
    wss_url = "wss://iqoption.com/echo/websocket"
    url_source = "referencia_iqoptionapi"
    library_version = None
    try:
        import iqoptionapi.api as iq_api_module
        import inspect, re
        source = inspect.getsource(iq_api_module.IQOptionAPI.__init__)
        match = re.search(r'wss://\{host\}(/[^\"\']+)', source)
        if match:
            wss_url = "wss://iqoption.com" + match.group(1)
            url_source = "codigo_instalado"
        else:
            url_source = "referencia_nao_confirmada_no_codigo_instalado"
        try:
            from importlib.metadata import version
            library_version = version("iqoptionapi")
        except Exception:
            pass
    except Exception:
        url_source = "referencia_biblioteca_indisponivel"
    host = urlsplit(wss_url).hostname
    results = {}
    for target in ("iqoption.com", host):
        if target in results:
            continue
        result = {"dns": False, "tcp_443": False, "tls": False}
        try:
            socket.getaddrinfo(target, 443, type=socket.SOCK_STREAM)
            result["dns"] = True
            with socket.create_connection((target, 443), timeout=4) as raw:
                result["tcp_443"] = True
                raw.settimeout(4)
                with ssl.create_default_context().wrap_socket(raw, server_hostname=target):
                    result["tls"] = True
        except Exception as exc:
            result["error_type"] = type(exc).__name__
        results[target] = result
    ws = {"attempted": False, "handshake_ok": False}
    try:
        import websocket
        ws["attempted"] = True
        connection = websocket.create_connection(wss_url, timeout=5, enable_multithread=True)
        try:
            ws["handshake_ok"] = True
        finally:
            connection.close()
    except Exception as exc:
        ws["error_type"] = type(exc).__name__
        # Nunca publicar mensagens de exceção que possam conter cookies ou credenciais.
        ws["note"] = "Handshake não concluído; sem login, a corretora pode recusar a conexão."
    return {"note": "Teste sem autenticação; sucesso no handshake não garante login.",
            "iq_library": IQ_Option is not None,
            "library_version": library_version,
            "websocket_url": wss_url, "url_source": url_source,
            "targets": results, "websocket": ws}

MIN_CANDLES = 75
PAIRS = {"EUR/USD":"EURUSD", "GBP/USD":"GBPUSD", "USD/JPY":"USDJPY", "EUR/JPY":"EURJPY", "GBP/JPY":"GBPJPY", "AUD/USD":"AUDUSD", "USD/CAD":"USDCAD"}

class Login(BaseModel):
    email: str
    password: str

class Setting(BaseModel):
    mode: str = "RECOVERY"

def session(request: Request, required=True):
    token=request.cookies.get("mega_iq_session", "")
    with LOCK:
        s=SESSIONS.get(token)
        if s and time.time()-s["created"]>SESSION_TTL:
            SESSIONS.pop(token,None);s=None
    if required and not s:raise HTTPException(401,"Conecte sua conta IQ Option primeiro.")
    return s

def call_timeout(fn, *args, seconds=12):
    return POOL.submit(fn,*args).result(timeout=seconds)

def install_iq_websocket_timeout():
    """Remove o loop infinito de abertura do WebSocket da iqoptionapi antiga."""
    if IQ_Option is None:
        return
    try:
        from iqoptionapi.api import IQOptionAPI
        from iqoptionapi.ws.client import WebsocketClient
        from iqoptionapi import global_value
        import inspect
        if getattr(IQOptionAPI.start_websocket, "_mega_bounded", False):
            return

        def bounded_start_websocket(self):
            global_value.check_websocket_if_connect = None
            global_value.check_websocket_if_error = False
            global_value.websocket_error_reason = None
            self.websocket_client = WebsocketClient(self)
            self.websocket_thread = threading.Thread(
                target=self.websocket.run_forever,
                kwargs={"sslopt": {"check_hostname": False, "cert_reqs": ssl.CERT_NONE}},
                daemon=True,
            )
            self.websocket_thread.start()
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                if global_value.check_websocket_if_error:
                    return False, str(global_value.websocket_error_reason or "Erro WebSocket")[:180]
                if global_value.check_websocket_if_connect == 0:
                    return False, "WebSocket encerrado pelo servidor"
                if global_value.check_websocket_if_connect == 1:
                    return True, None
                if not self.websocket_thread.is_alive():
                    return False, "Thread WebSocket terminou sem conectar"
                time.sleep(0.1)
            try:
                self.websocket.close()
            except Exception:
                pass
            return False, "WebSocket não abriu em 12s (rede, handshake ou bloqueio do servidor)"

        # Compatível com a implementação clássica (flags globais) da biblioteca.
        source = inspect.getsource(IQOptionAPI.start_websocket)
        if "global_value.check_websocket_if_connect" not in source or "self.websocket_client" not in source:
            LOG.warning("[IQ CONNECTOR] versão diferente: patch WebSocket não aplicado")
            return
        bounded_start_websocket._mega_bounded = True
        IQOptionAPI.start_websocket = bounded_start_websocket
        LOG.warning("[IQ CONNECTOR] timeout interno WebSocket 12s ativado")
    except Exception as exc:
        LOG.warning("[IQ CONNECTOR] não foi possível ativar timeout interno: %s", type(exc).__name__)

# A versão 3.99.72 utilizava o start_websocket original da iqoptionapi.
# Não substituir globalmente o método: o patch antigo alterava a negociação
# do WebSocket e desabilitava a validação TLS, dificultando a autenticação.
# O timeout externo de /iq-login continua ativo.
LOG.info("[IQ CONNECTOR] WebSocket original da biblioteca preservado")

# Registra apenas metadados de respostas inválidas, nunca corpo, cookies ou credenciais.
def install_iq_json_diagnostics():
    try:
        import requests
        original = requests.Response.json
        if getattr(original, "_mega_json_diagnostics", False):
            return
        def json_with_diagnostics(response, *args, **kwargs):
            try:
                return original(response, *args, **kwargs)
            except ValueError:
                from urllib.parse import urlsplit
                from threading import current_thread
                # Não registra URL completa: query strings podem conter segredos.
                host = urlsplit(response.url or "").hostname or "desconhecido"
                content_type = (response.headers.get("Content-Type") or "desconhecido").split(";")[0][:64]
                LOG.error("[IQ JSON] thread=%s host=%s http_status=%s content_type=%s bytes=%s",
                          current_thread().name[:40], host, response.status_code,
                          content_type, len(response.content or b""))
                raise
        json_with_diagnostics._mega_json_diagnostics = True
        requests.Response.json = json_with_diagnostics
    except Exception as exc:
        LOG.warning("[IQ JSON] diagnóstico indisponível: %s", type(exc).__name__)

install_iq_json_diagnostics()

def install_iq_reason_parser_fix():
    """Evita JSONDecodeError quando a biblioteca tenta json.loads(reason).

    Um motivo de erro não-JSON continua sendo uma falha de login, nunca sucesso.
    Aplica-se apenas à função connect da versão instalada, se contiver o padrão.
    """
    if IQ_Option is None:
        return
    try:
        import inspect, textwrap, json
        original = IQ_Option.connect
        if getattr(original, "_mega_reason_fixed", False):
            return
        source = textwrap.dedent(inspect.getsource(original))
        needle = "json.loads(reason)"
        if needle not in source:
            LOG.info("[IQ REASON] versão não usa json.loads(reason); sem alteração")
            return

        def safe_reason_json(reason):
            try:
                parsed = json.loads(reason)
            except (ValueError, TypeError):
                # A biblioteca acessa ['code']; retornar código neutro evita
                # o crash, mas mantém a decisão de autenticação como falha.
                LOG.warning("[IQ REASON] resposta de erro não-JSON; tipo=%s tamanho=%s",
                            type(reason).__name__, len(str(reason or "")))
                return {"code": "unexpected_auth_response"}
            if not isinstance(parsed, dict):
                LOG.warning("[IQ REASON] resposta de erro JSON não é objeto")
                return {"code": "unexpected_auth_response"}
            return parsed

        namespace = dict(original.__globals__)
        namespace["_mega_safe_reason_json"] = safe_reason_json
        source = source.replace(needle, "_mega_safe_reason_json(reason)")
        exec(compile(source, "<iqoptionapi-connect-reason-fix>", "exec"), namespace)
        fixed = namespace["connect"]
        fixed._mega_reason_fixed = True
        IQ_Option.connect = fixed
        LOG.warning("[IQ REASON] correção para motivo de autenticação não-JSON ativada")
    except Exception as exc:
        LOG.warning("[IQ REASON] correção não aplicada: %s", type(exc).__name__)

install_iq_reason_parser_fix()

def iq_error_code(reason):
    """Classificação sem expor texto de resposta, e-mail, senha ou tokens."""
    msg = str(reason or "").lower()
    if any(x in msg for x in ("invalid_credentials", "invalid credentials", "wrong password", "incorrect password")):
        return "invalid_credentials"
    if any(x in msg for x in ("2fa", "two_factor", "two-factor", "verification code")):
        return "verification_required"
    if any(x in msg for x in ("captcha", "cloudflare", "challenge", "403", "forbidden", "access denied")):
        return "security_challenge_or_access_denied"
    if any(x in msg for x in ("429", "too many", "rate limit")):
        return "rate_limited"
    if any(x in msg for x in ("timed out", "timeout", "time out")):
        return "network_timeout"
    if any(x in msg for x in ("websocket", "handshake", "connection closed")):
        return "websocket_failure"
    if any(x in msg for x in ("json", "expecting value", "unexpected_auth_response")):
        return "unexpected_auth_response"
    return "unknown_connection_failure"

def login_blocking(email, password, cancelled, started):
    iq = None
    try:
        iq_login_stage("creating_client", started=started)
        iq = IQ_Option(email.strip(), password)
        if cancelled.is_set():
            return None
        iq_login_stage("connect_authentication_websocket")
        # A biblioteca executa autenticação e abertura de WebSocket dentro de connect().
        # Sem instrumentar a biblioteca não é possível distinguir essas subetapas.
        connect_result = iq.connect()
        if isinstance(connect_result, (tuple, list)):
            ok = bool(connect_result[0]) if connect_result else False
            reason = connect_result[1] if len(connect_result) > 1 else ""
        else:
            ok, reason = bool(connect_result), ""
        if cancelled.is_set():
            iq_login_stage("late_response_discarded", "timed_out")
            return None
        if not ok:
            iq_login_stage("connection_rejected", "failed")
            code = iq_error_code(reason)
            with IQ_LOGIN_STATE_LOCK:
                IQ_LOGIN_STATE["error_code"] = code
            LOG.warning("[IQ CONNECTOR] connect retornou falha classificada=%s tipo=%s", code, type(reason).__name__)
            raise RuntimeError("Falha na conexão IQ: " + code)
        iq_login_stage("checking_websocket")
        if not iq.check_connect():
            iq_login_stage("websocket_not_confirmed", "failed")
            raise RuntimeError("IQ não confirmou o WebSocket")
        if cancelled.is_set():
            iq_login_stage("late_response_discarded", "timed_out")
            return None
        with IQ_LOGIN_STATE_LOCK:
            IQ_LOGIN_STATE["error_code"] = None
        iq_login_stage("connected", "success")
        return iq
    finally:
        if cancelled.is_set() and iq is not None:
            try:
                iq.close_connect()
            except Exception:
                pass

@app.post('/iq-login')
def iq_login(body: Login):
    if IQ_Option is None:
        raise HTTPException(503, "Biblioteca iqoptionapi não instalada no servidor.")
    # Bloqueia tentativas simultâneas: uma chamada presa não deve criar outras.
    if not IQ_LOGIN_GUARD.acquire(blocking=False):
        raise HTTPException(429, "Uma tentativa IQ ainda está em andamento. Aguarde a conclusão antes de tentar novamente.")
    started = time.monotonic()
    cancelled = threading.Event()
    try:
        with IQ_LOGIN_STATE_LOCK:
            IQ_LOGIN_STATE["error_code"] = None
        iq_login_stage("queued", started=started)
        future = POOL.submit(login_blocking, body.email, body.password, cancelled, started)
        IQ_LOGIN_WORKER["future"] = future
        # O guard só é liberado quando o worker realmente terminar, mesmo após timeout.
        future.add_done_callback(lambda _future: IQ_LOGIN_GUARD.release())
        try:
            iq = future.result(timeout=LOGIN_TIMEOUT)
        except FutureTimeout:
            cancelled.set()
            with IQ_LOGIN_STATE_LOCK:
                stage = IQ_LOGIN_STATE.get("stage", "unknown")
            iq_login_stage(stage, "timed_out_worker_may_still_be_running")
            LOG.warning("[IQ CONNECTOR] timeout %ss etapa=%s; aguardando worker encerrar", LOGIN_TIMEOUT, stage)
            raise HTTPException(504, "IQ não respondeu em 35 segundos; etapa: " + stage + ". Consulte /iq-diagnostics.")
        except Exception as exc:
            code = iq_error_code(exc)
            with IQ_LOGIN_STATE_LOCK:
                if IQ_LOGIN_STATE.get("error_code") is None:
                    IQ_LOGIN_STATE["error_code"] = code
            iq_login_stage("connection_error", "failed_" + type(exc).__name__)
            # Somente nomes de funções e módulos: traceback completo pode expor dados.
            if type(exc).__name__ == "JSONDecodeError":
                import traceback
                frames = traceback.extract_tb(exc.__traceback__)
                trace = " > ".join(f"{os.path.basename(f.filename)}:{f.name}:{f.lineno}" for f in frames[-9:])
                LOG.error("[IQ JSON] origem=%s", trace[:650])
            LOG.warning("[IQ CONNECTOR] falha tipo=%s detalhe=%s", type(exc).__name__, str(exc)[:180])
            raise HTTPException(502, "Falha IQ: " + IQ_LOGIN_STATE.get("error_code", "unknown_connection_failure") + ". Consulte /iq-diagnostics e os logs.")
        if iq is None:
            raise HTTPException(504, "A tentativa IQ expirou.")
        token = secrets.token_urlsafe(40)
        with LOCK:
            SESSIONS[token] = {"iq": iq, "created": time.time(), "mode": "RECOVERY", "last_signal": {}, "active": {}, "history": [], "last_poll": {}, "last_error": ""}
        response = JSONResponse({"connected": True, "message": "IQ Option conectada"})
        response.set_cookie("mega_iq_session", token, httponly=True, secure=True, samesite="lax", max_age=SESSION_TTL)
        return response
    except HTTPException:
        raise
    # Guard pertence ao callback do Future; só liberar diretamente se submit falhou.
    except Exception:
        if 'future' not in locals():
            IQ_LOGIN_GUARD.release()
        raise

@app.post('/iq-logout')
def iq_logout(request:Request):
    token=request.cookies.get("mega_iq_session","")
    with LOCK:s=SESSIONS.pop(token,None)
    if s:
        try:s["iq"].close_connect()
        except Exception:pass
    response=JSONResponse({"connected":False})
    response.delete_cookie("mega_iq_session")
    return response

@app.get('/iq-status')
def iq_status(request:Request):
    s=session(request,False)
    if not s:return {"connected":False,"source":"NONE"}
    try:connected=bool(call_timeout(s["iq"].check_connect,seconds=3))
    except Exception:connected=False
    return {"connected":connected,"source":"IQ_OPTION" if connected else "NONE","error":s["last_error"]}

def fetch_candles(s,symbol,count=180):
    iq=s['iq']
    if not iq.check_connect():raise RuntimeError("WebSocket IQ desconectado. Faça novo login.")
    code=PAIRS.get(symbol)
    if not code:raise ValueError("Par não permitido")
    raw=iq.get_candles(code,60,count,int(time.time()))
    if not raw or len(raw)<MIN_CANDLES:raise RuntimeError("IQ retornou poucos candles M1")
    rows=[]
    for c in raw:
        t=int(c.get('from',c.get('at',0)))
        if not t:continue
        rows.append({"timestamp":t,"open":float(c['open']),"high":float(c['max']),"low":float(c['min']),"close":float(c['close'])})
    rows=sorted({r['timestamp']:r for r in rows}.values(),key=lambda x:x['timestamp'])
    now=time.time()
    rows=[r for r in rows if r['timestamp']+60<=now-1]
    if len(rows)<MIN_CANDLES:raise RuntimeError("Não há 75 candles M1 fechados")
    if now-rows[-1]['timestamp']>180:raise RuntimeError("Candles IQ atrasados mais de 3 minutos")
    # Bloqueia preenchimento de lacunas artificiais; intervalos sem dados não são inventados.
    recent=rows[-20:]
    if any(b['timestamp']-a['timestamp']!=60 for a,b in zip(recent,recent[1:])):
        raise RuntimeError("Candles IQ com lacunas nos últimos 20 minutos")
    if len({(r['open'],r['high'],r['low'],r['close']) for r in recent})<3:
        raise RuntimeError("Candles IQ repetidos/suspeitos")
    return rows

@app.get('/candles')
def candles(request:Request,symbol:str='EUR/USD',limit:int=10):
    s=session(request)
    try:
        rows=call_timeout(fetch_candles,s,symbol,max(100,min(600,limit)),seconds=14)
        s['last_error']=''
        return {"ok":True,"source":"IQ_OPTION","symbol":symbol,"interval":"1min","candles":rows[-max(1,min(limit,100)):],"count":len(rows)}
    except Exception as exc:
        s['last_error']=str(exc)[:150]
        raise HTTPException(503,"Fonte IQ indisponível: "+s['last_error'])

# Único indicador preservado do aplicativo anterior: Scalping M1 FLEX.
_SCALPINGM1_LOCK=threading.RLock()
_SCALPINGM1_LAST={}
def scalping_m1_flex_strategy(cs, symbol="EUR/USD", timeframe="1min", market="OPEN"):
    base={"available":True,"engine":"SCALPINGM1","strategy":"Mega Bot","provider":"LOCAL_MTF_BREAK_RETEST","direction":"NEUTRO","confirmed":False,"confidence":0.0,"confidence_is_probability":False,"next_candle_entry":True,"expiry_candles":1,"direct_win_only":True,"gale_signal":False,"martingale":False,"non_repaint":True,"prealert_seconds":20,"reason":"Monitorando rompimento M5 ou impulso M1."}
    if str(timeframe) not in ("1min","M1","60"):
        return {**base,"reason":"Scalping M1 FLEX funciona no timeframe M1."}
    rows=list(cs or [])[-600:]
    if len(rows)<75:return {**base,"reason":"Aguardando pelo menos 75 candles M1."}
    try:
        def ts(row):
            v=row.get('timestamp',row.get('time',row.get('datetime')))
            if isinstance(v,(int,float)):
                return int(v/1000 if v>1e11 else v)
            if isinstance(v,str):
                try:return int(float(v)/1000 if float(v)>1e11 else float(v))
                except ValueError:return int(datetime.fromisoformat(v.replace('Z','+00:00')).timestamp())
            raise ValueError('timestamp ausente')
        # Reagrupa somente candles M1 efetivamente fechados; nunca usa vela futura.
        def aggregate(minutes):
            groups={}
            for row in rows:
                t=ts(row); bucket=t//(minutes*60)
                groups.setdefault(bucket,[]).append((t,row))
            out=[]
            for bucket,values in sorted(groups.items()):
                values.sort(key=lambda z:z[0]); unique={t:r for t,r in values}
                if len(unique)<minutes:continue
                ordered=[unique[t] for t in sorted(unique)]
                out.append({'bucket':bucket,'open':float(ordered[0]['open']),'close':float(ordered[-1]['close']),'high':max(float(r['high']) for r in ordered),'low':min(float(r['low']) for r in ordered)})
            return out
        m15=aggregate(15);m5=aggregate(5)
        if len(m15)<2 or len(m5)<3:return {**base,'reason':'Aguardando candles completos M15 e M5.'}
        now_bucket=ts(rows[-1])//60
        last=rows[-1];price=float(last['close']);rng=max(float(last['high'])-float(last['low']),1e-12)
        # FLEX: rompimento confirmado M5 OU impulso M1 após rompimento recente.
        # Somente candles fechados; M15 serve de contexto, não bloqueio obrigatório.
        recent_m5=m5[-4:]
        candidates=[]
        for i in range(1,len(recent_m5)):
            prev=recent_m5[i-1]; cur=recent_m5[i]
            cr=max(cur['high']-cur['low'],1e-12)
            body=abs(cur['close']-cur['open'])/cr
            if body<0.28:continue
            direction='CALL' if cur['close']>prev['high'] and cur['close']>cur['open'] else 'PUT' if cur['close']<prev['low'] and cur['close']<cur['open'] else None
            if direction:candidates.append((cur['bucket'],direction,prev['high'] if direction=='CALL' else prev['low']))
        last_open=float(last['open']);last_high=float(last['high']);last_low=float(last['low'])
        body=abs(price-last_open)/rng
        # Impulso de M1: rompe máxima/mínima das 5 velas anteriores.
        prior5=rows[-6:-1]
        high5=max(float(r['high']) for r in prior5)
        low5=min(float(r['low']) for r in prior5)
        impulse='CALL' if price>high5 and price>last_open and body>=0.48 else 'PUT' if price<low5 and price<last_open and body>=0.48 else None
        breakout,direction,level=max(candidates,key=lambda x:x[0]) if candidates else (None,None,None)
        m5_fresh=breakout is not None and 0<=now_bucket-(breakout+1)*5<=5
        m5_ok=False
        if m5_fresh:
            tolerance=max(rng*.45,abs(level)*.00003)
            touched=(last_low<=level+tolerance and price>=level-tolerance) if direction=='CALL' else (last_high>=level-tolerance and price<=level+tolerance)
            continuation=(price>last_open and body>=.42) if direction=='CALL' else (price<last_open and body>=.42)
            m5_ok=touched or continuation
        if impulse:
            direction=impulse
            mode='impulso M1'
        elif m5_ok:
            mode='rompimento M5 com continuação/reteste M1'
        else:
            return {**base,'reason':'Monitorando rompimento M5 ou impulso M1 confirmado.'}
        event=f'SCALPINGM1:{market}:{symbol}:{direction}:{breakout}:{now_bucket}'
        return {**base,'direction':direction,'confirmed':True,'confidence':60.0,'risk':'HIGH','event_key':event,'reason':f'Mega Bot {direction}: {mode}; próxima vela.'}
    except Exception as exc:return {**base,'reason':f'Scalping M1 aguardando dados M1 válidos: {str(exc)[:90]}'}

def settle(s,rows,symbol):
    """Apuração virtual pelo candle de entrada; recuperação só avança em NOVO sinal."""
    chain=s['active'].get(symbol)
    if not chain or chain.get('pending') is None:return
    p=chain['pending']
    candle=next((c for c in rows if c['timestamp']==p['entry_ts']),None)
    if candle is None:return
    direction=p['direction']
    win=(candle['close']>candle['open']) if direction=='CALL' else (candle['close']<candle['open'])
    draw=candle['close']==candle['open']
    if draw:chain['pending']=None;chain['waiting']=True;return
    chain['steps'].append({'stage':p['stage'],'direction':direction,'timestamp':p['entry_ts'],'result':'WIN' if win else 'LOSS'})
    chain['pending']=None
    if win:
        chain['final']='WIN DIRETO' if p['stage']==0 else f'WIN REC {p["stage"]}'
    elif p['stage']>=2 or (s['mode']=='GALE1' and p['stage']>=1):
        chain['final']='LOSS FINAL'
    else:
        chain['waiting']=True
    if chain.get('final'):
        s['history'].append({'symbol':symbol,'final':chain['final'],'steps':chain['steps'],'finished_at':int(time.time())})
        s['active'].pop(symbol,None)

def maybe_register(s,symbol,decision,rows):
    if decision['direction'] not in ('CALL','PUT') or not decision.get('confirmed'):return decision
    stamp=rows[-1]['timestamp']
    # Um evento por candle, com trava de 4 minutos entre entradas INICIAIS.
    event=(symbol,stamp,decision['direction'])
    if event in s['last_poll']:return {**decision,'reason':'Sinal já registrado neste candle','already_registered':True}
    chain=s['active'].get(symbol)
    if chain and chain.get('pending') is not None:return {**decision,'direction':'NEUTRO','confirmed':False,'reason':'Aguardando apuração da entrada anterior'}
    if chain and chain.get('waiting'):
        stage=len(chain['steps'])
        if stage>2:return decision
        chain['pending']={'stage':stage,'entry_ts':stamp+60,'direction':decision['direction']}
        chain['waiting']=False
    else:
        last=s['last_signal'].get(symbol,0)
        if stamp-last<240:return {**decision,'direction':'NEUTRO','confirmed':False,'reason':'Intervalo de 4 minutos entre sinais'}
        s['last_signal'][symbol]=stamp
        s['active'][symbol]={'steps':[],'pending':{'stage':0,'entry_ts':stamp+60,'direction':decision['direction']},'waiting':False}
    s['last_poll'][event]=time.time()
    return {**decision,'entry_timestamp':stamp+60,'registered':True}

@app.get('/signal-ai')
def signal(request:Request,symbol:str='EUR/USD',interval:str='1min'):
    s=session(request)
    if interval!='1min':raise HTTPException(400,'Somente M1')
    try:
        rows=call_timeout(fetch_candles,s,symbol,200,seconds=14)
        with LOCK:
            settle(s,rows,symbol)
            result=scalping_m1_flex_strategy(rows,symbol=symbol,timeframe='1min',market='OPEN')
            result=maybe_register(s,symbol,result,rows)
        return {'ok':True,'source':'IQ_OPTION','symbol':symbol,'candle_timestamp':rows[-1]['timestamp'],**result}
    except Exception as exc:
        s['last_error']=str(exc)[:150]
        return {'ok':False,'source':'NONE','direction':'NEUTRO','confidence':0,'reason':'AGUARDANDO DADOS: '+s['last_error']}

@app.get('/performance')
def performance(request:Request):
    s=session(request)
    history=s['history']
    wins=sum(x['final'].startswith('WIN') for x in history)
    losses=sum(x['final']=='LOSS FINAL' for x in history)
    return {'wins':wins,'losses':losses,'total':len(history),'accuracy':round(100*wins/len(history),1) if history else 0,'history':history[-200:],'active':s['active']}

@app.post('/management')
def management(request:Request,body:Setting):
    s=session(request)
    if body.mode not in ('RECOVERY','GALE1','GALE2'):raise HTTPException(400,'Modo inválido')
    if s['active']:raise HTTPException(409,'Aguarde concluir a sequência atual antes de trocar o modo.')
    s['mode']=body.mode
    return {'mode':s['mode']}

@app.get('/health')
def health():return {'ok':True,'version':'2.0.0','iq_library':IQ_Option is not None}

@app.get('/',response_class=HTMLResponse)
def index():
    with open(os.path.join(os.path.dirname(__file__),'index.html'),encoding='utf8') as f:return HTMLResponse(f.read())
