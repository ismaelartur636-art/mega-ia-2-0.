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
def prepare_iq_certificate():
    """Disponibiliza o CA bundle esperado pela biblioteca sem substituir arquivo existente."""
    from pathlib import Path
    target = Path.cwd() / "cacert.pem"
    try:
        if target.exists():
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.load_verify_locations(cafile=str(target))
            return {"ready": True, "source": "existing", "created": False}
        import certifi
        source = Path(certifi.where())
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cafile=str(source))
        import tempfile
        with tempfile.NamedTemporaryFile(dir=target.parent, prefix="iq-ca-", delete=False) as temp:
            temporary = Path(temp.name)
            temp.write(source.read_bytes())
        try:
            os.chmod(temporary, 0o644)
            os.link(temporary, target)
        except FileExistsError:
            pass
        finally:
            temporary.unlink(missing_ok=True)
        context.load_verify_locations(cafile=str(target))
        LOG.warning("[IQ CERT] cacert.pem disponível e validado")
        return {"ready": True, "source": "certifi", "created": True}
    except Exception as exc:
        LOG.error("[IQ CERT] preparo falhou: %s", type(exc).__name__)
        return {"ready": False, "error_type": type(exc).__name__}

IQ_CERTIFICATE_SETUP = prepare_iq_certificate()

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

CURRENCIES = set("EUR USD GBP JPY AUD CAD CHF NZD BRL MXN ZAR TRY NOK SEK DKK SGD HKD PLN RUB INR CNY CNH AED THB KRW IDR".split())
CRYPTO_BASES = set("BTC ETH LTC XRP BCH BNB ADA SOL DOGE DOT TRX EOS ETC LINK MATIC AVAX TON ATOM DASH XLM UNI APT NEAR FIL SHIB ICP USDT PEPE SUI ARB OP".split())

def describe_iq_asset(code, kind=None):
    code = str(code)
    otc = "OTC" in code.upper()
    core = re.sub(r"(?:-OTC|-op|-L)$", "", code, flags=re.I).upper()
    compact = core.replace("/", "").replace("-", "")
    forex = len(compact) == 6 and compact[:3] in CURRENCIES and compact[3:] in CURRENCIES
    crypto = kind == "crypto" or any(compact == base + quote for base in CRYPTO_BASES for quote in ("USD", "EUR", "USDT", "BTC")) or any(word in core for word in ("BITCOIN", "ETHEREUM", "DOGECOIN", "LITECOIN", "CARDANO", "SOLANA", "RIPPLE", "POLKADOT", "CHAINLINK", "TRON"))
    if kind == "forex": forex = True
    if forex:
        label = compact[:3] + "/" + compact[3:]
        group = "FOREX_OTC" if otc else "FOREX_OPEN"
    elif crypto:
        label = core
        for quote in ("USDT", "USD", "EUR", "BTC"):
            if core.endswith(quote) and len(core) > len(quote):
                label = core[:-len(quote)] + "/" + quote
                break
        group = "CRYPTO_OTC" if otc else "CRYPTO_OPEN"
    elif otc:
        label, group = core, "OTHER_OTC"
    else:
        return None
    if otc: label += " OTC"
    if code.lower().endswith("-op"): label += " (opções)"
    if code.endswith("-L"): label += " (cripto)"
    return {"symbol": code, "code": code, "label": label, "group": group,
            "market": "IQ_OTC" if otc else "OPEN", "open": None}

def initial_iq_catalog(s):
    records = {}
    for code, active_id in s["iq"].get_all_ACTIVES_OPCODE().items():
        item = describe_iq_asset(code)
        if item and isinstance(active_id, int) and active_id > 0:
            records[code] = {**item, "active_id": active_id}
    return records

def refresh_iq_catalog(s):
    """Consultas serializadas e limitadas, sem reconectar nem alterar autenticação."""
    records = initial_iq_catalog(s)
    errors = []
    iq = s["iq"]
    api = iq.api
    def await_field(name, seconds=6):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = getattr(api, name, None)
            if value is not None: return value
            if not iq.check_connect(): raise RuntimeError("IQ desconectada")
            time.sleep(.05)
        raise TimeoutError("Consulta de ativos expirou")
    def add(code, active_id, kind=None, opened=None):
        item = describe_iq_asset(code, kind)
        try: active_id = int(active_id)
        except (ValueError, TypeError): return
        if not item or active_id <= 0: return
        previous = records.get(code)
        if previous and previous.get("open") is True: opened = True
        records[code] = {**item, "active_id": active_id, "open": opened}
    with s.setdefault("data_lock", threading.RLock()):
        if not iq.check_connect(): raise RuntimeError("Conecte à IQ primeiro")
        try:
            api.api_option_init_all_result_v2 = None
            api.get_api_option_init_all_v2()
            data = await_field("api_option_init_all_result_v2")
            data = data.get("result", data)
            for family in ("binary", "turbo"):
                for active_id, detail in data.get(family, {}).get("actives", {}).items():
                    code = str(detail.get("name", "")).split(".")[-1]
                    add(code, active_id, opened=bool(detail.get("enabled") and not detail.get("is_suspended", False)))
        except Exception as exc: errors.append("options:" + type(exc).__name__)
        for kind in ("forex", "crypto"):
            try:
                api.instruments = None
                api.get_instruments(kind)
                data = await_field("instruments")
                for detail in data.get("instruments", []):
                    opened = any(float(t.get("open", 0)) <= time.time() < float(t.get("close", 0)) for t in detail.get("schedule", [])) if "schedule" in detail else None
                    add(str(detail["id"]), detail["active_id"], kind, opened)
            except Exception as exc: errors.append(kind + ":" + type(exc).__name__)
        try:
            api.underlying_list_data = None
            api.get_digital_underlying()
            data = await_field("underlying_list_data")
            for detail in data.get("underlying", []):
                code = str(detail.get("underlying", ""))
                active_id = detail.get("active_id", records.get(code, {}).get("active_id"))
                opened = any(float(t.get("open", 0)) <= time.time() < float(t.get("close", 0)) for t in detail.get("schedule", [])) if "schedule" in detail else None
                add(code, active_id, opened=opened)
        except Exception as exc: errors.append("digital:" + type(exc).__name__)
        from iqoptionapi import constants
        with LOCK:
            for code, detail in records.items():
                constants.ACTIVES[code] = detail["active_id"]
            s["asset_catalog"] = records
            s["asset_catalog_time"] = time.time()
            s["asset_catalog_errors"] = errors
    return records

@app.get('/iq-assets')
def iq_assets(request: Request, refresh: bool=False):
    s = session(request)
    with LOCK:
        if "asset_catalog" not in s:
            s["asset_catalog"] = initial_iq_catalog(s)
        future = s.get("asset_catalog_future")
        if future and future.done():
            try: future.result()
            except Exception as exc: s["asset_catalog_errors"] = [type(exc).__name__]
        age = time.time() - s.get("asset_catalog_attempt", 0)
        if (future is None or future.done()) and age >= (30 if refresh else 300):
            s["asset_catalog_attempt"] = time.time()
            future = POOL.submit(refresh_iq_catalog, s)
            s["asset_catalog_future"] = future
        rows = [dict(x) for x in s["asset_catalog"].values()]
    return {"source": "IQ_OPTION", "assets": sorted(rows, key=lambda x:(x["group"], x["label"], x["code"])),
            "loading": bool(future and not future.done()), "updated_at": s.get("asset_catalog_time"),
            "errors": s.get("asset_catalog_errors", []), "count": len(rows)}

def resolve_iq_asset(s, symbol):
    catalog = s.get("asset_catalog") or initial_iq_catalog(s)
    code = PAIRS.get(symbol, symbol)
    item = catalog.get(code)
    if not item: raise ValueError("Ativo não informado pela IQ. Atualize a lista de ativos.")
    if item.get("open") is False: raise ValueError("Ativo fechado na IQ Option")
    return item

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
        fixed._mega_source = source
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

IQ_CONNECT_CONTEXT = threading.local()

def iq_wait_checkpoint(stage):
    context = getattr(IQ_CONNECT_CONTEXT, "attempt", None)
    if context is None:
        return
    if context["stage"] != stage:
        context["stage"] = stage
        iq_login_stage(stage)
    if context["cancelled"].is_set() or time.monotonic() >= context["deadline"]:
        raise TimeoutError("IQ aguardando resposta em " + stage)
    time.sleep(0.02)

def install_iq_internal_deadlines():
    """Acrescenta prazo às esperas, preservando o protocolo da versão instalada."""
    if IQ_Option is None:
        return
    import ast, inspect, textwrap
    from iqoptionapi.api import IQOptionAPI
    for cls, method in ((IQOptionAPI, "start_websocket"),
                        (IQOptionAPI, "send_ssid"),
                        (IQOptionAPI, "connect"),
                        (IQ_Option, "connect")):
        original = getattr(cls, method, None)
        if original is None or getattr(original, "_mega_deadline", False):
            continue
        try:
            source = getattr(original, "_mega_source", None)
            if source is None:
                source = textwrap.dedent(inspect.getsource(original))
            tree = ast.parse(source)
            function = tree.body[0]
            if not isinstance(function, ast.FunctionDef):
                raise ValueError("unsupported method")
            function.decorator_list = []
            count = [0]
            class BoundLoops(ast.NodeTransformer):
                def visit_While(self, node):
                    self.generic_visit(node)
                    count[0] += 1
                    stage = "iq_" + method + "_wait_" + str(count[0])
                    checkpoint = ast.Expr(value=ast.Call(
                        func=ast.Name(id="_mega_wait_checkpoint", ctx=ast.Load()),
                        args=[ast.Constant(value=stage)], keywords=[]))
                    # Antes do try da biblioteca: não deixar except/pass engolir timeout.
                    node.body.insert(0, checkpoint)
                    return node
            BoundLoops().visit(function)
            if not count[0]:
                continue
            ast.fix_missing_locations(tree)
            namespace = dict(original.__globals__)
            namespace["_mega_wait_checkpoint"] = iq_wait_checkpoint
            exec(compile(tree, "<mega-iq-deadline-" + method + ">", "exec"), namespace)
            fixed = namespace[method]
            fixed._mega_deadline = True
            setattr(cls, method, fixed)
            LOG.info("[IQ DEADLINE] %s.%s: %s esperas limitadas", cls.__name__, method, count[0])
        except Exception as exc:
            LOG.error("[IQ DEADLINE] indisponível em %s.%s: %s", cls.__name__, method, type(exc).__name__)

install_iq_internal_deadlines()

def abort_iq_client(client):
    """Encerra HTTP/socket usando os métodos disponíveis na biblioteca instalada."""
    if client is None:
        return
    api = getattr(client, "api", None)
    for target in (client, api, getattr(api, "websocket", None)):
        for name in ("close_connect", "close"):
            close = getattr(target, name, None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
                break

def login_blocking(email, password, cancelled, started, attempt):
    iq = None
    accepted = False
    try:
        iq_login_stage("creating_client", started=started)
        iq = IQ_Option(email.strip(), password)
        attempt["client"] = iq
        if cancelled.is_set():
            return None
        iq_login_stage("connect_authentication_websocket")
        # A biblioteca executa autenticação e abertura de WebSocket dentro de connect().
        # Sem instrumentar a biblioteca não é possível distinguir essas subetapas.
        IQ_CONNECT_CONTEXT.attempt = {"deadline": started + LOGIN_TIMEOUT - 5, "cancelled": cancelled, "stage": None}
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
        accepted = True
        return iq
    finally:
        IQ_CONNECT_CONTEXT.attempt = None
        if not accepted:
            abort_iq_client(iq)

@app.post('/iq-login')
def iq_login(body: Login, request: Request):
    if IQ_Option is None:
        raise HTTPException(503, "Biblioteca iqoptionapi não instalada no servidor.")
    if not body.email.strip() or not body.password:
        raise HTTPException(400, "Informe e-mail e senha da IQ Option.")
    existing = session(request, required=False)
    if existing and existing.get("email") == body.email.strip().casefold():
        try:
            if call_timeout(existing["iq"].check_connect, seconds=3):
                return JSONResponse({"connected": True, "message": "Sessão IQ Option já conectada"})
        except Exception:
            pass
    # Bloqueia tentativas simultâneas: uma chamada presa não deve criar outras.
    if not IQ_LOGIN_GUARD.acquire(blocking=False):
        raise HTTPException(429, "Uma tentativa IQ ainda está em andamento. Aguarde a conclusão antes de tentar novamente.")
    started = time.monotonic()
    cancelled = threading.Event()
    attempt = {"client": None}
    try:
        with IQ_LOGIN_STATE_LOCK:
            IQ_LOGIN_STATE["error_code"] = None
        iq_login_stage("queued", started=started)
        future = POOL.submit(login_blocking, body.email, body.password, cancelled, started, attempt)
        IQ_LOGIN_WORKER["future"] = future
        # O guard só é liberado quando o worker realmente terminar, mesmo após timeout.
        future.add_done_callback(lambda _future: IQ_LOGIN_GUARD.release())
        try:
            iq = future.result(timeout=LOGIN_TIMEOUT)
        except FutureTimeout:
            cancelled.set()
            threading.Thread(target=abort_iq_client, args=(attempt["client"],), daemon=True, name="iq-timeout-close").start()
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
            SESSIONS[token] = {"iq": iq, "email": body.email.strip().casefold(), "created": time.time(), "mode": "RECOVERY", "last_signal": {}, "active": {}, "history": [], "last_poll": {}, "last_error": ""}
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
    asset=resolve_iq_asset(s,symbol)
    code=asset["code"]
    with s.setdefault("data_lock", threading.RLock()):
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
            result=scalping_m1_flex_strategy(rows,symbol=symbol,timeframe='1min',market='IQ_OTC' if 'OTC' in symbol.upper() else 'OPEN')
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
    return HTMLResponse(APP_HTML)

@app.get('/iq-files-check')
def iq_files_check():
    """Somente leitura: não exibe credenciais, variáveis ou conteúdo dos arquivos."""
    import importlib.metadata
    import inspect
    from pathlib import Path
    result = {"diagnostic_version": "files-2", "certificate_setup": IQ_CERTIFICATE_SETUP, "iq_library_loaded": IQ_Option is not None}
    versions = {}
    for name in ("websocket-client", "requests", "certifi", "iqoptionapi"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = "not_found"
    result["versions"] = versions
    def certificate_status(path):
        path = Path(path)
        exists = path.is_file()
        info = {"exists": exists, "readable": exists and os.access(path, os.R_OK)}
        if info["readable"]:
            try:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.load_verify_locations(cafile=str(path))
                info["valid_ca_bundle"] = True
            except Exception as exc:
                info["valid_ca_bundle"] = False
                info["error_type"] = type(exc).__name__
        return info
    result["certificates"] = {
        "working_directory_cacert": certificate_status(Path.cwd() / "cacert.pem"),
        "main_directory_cacert": certificate_status(Path(__file__).resolve().parent / "cacert.pem"),
        "render_secret_cacert": certificate_status("/etc/secrets/cacert.pem"),
    }
    try:
        import certifi
        result["certificates"]["certifi_bundle"] = certificate_status(certifi.where())
    except ImportError:
        result["certificates"]["certifi_bundle"] = {"exists": False}
    try:
        from iqoptionapi import api as iq_api
        source = inspect.getsource(iq_api)
        result["library_references_relative_cacert"] = "cacert.pem" in source
        result["internal_deadlines"] = {
            name: bool(getattr(getattr(iq_api.IQOptionAPI, name, None), "_mega_deadline", False))
            for name in ("start_websocket", "send_ssid", "connect")
        }
    except Exception as exc:
        result["inspection_error_type"] = type(exc).__name__
    result["note"] = "Certificado ausente é uma pista; este teste não comprova a causa do timeout de rede."
    return result

APP_HTML = '<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Mega IA Trader • Parceiro</title><style>\n:root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:#021123;color:#e8f4ff;font:14px system-ui,Arial}header{background:#03152a;padding:20px 14px;border-bottom:1px solid #164e79}h1{margin:0;font-size:27px}small,.sub{color:#8faac3;font-size:11px}main{max-width:850px;margin:auto;padding:12px}.panel{background:linear-gradient(145deg,#071c33,#031326);border:1px solid #174468;border-radius:15px;padding:15px;margin:10px 0}.row{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap}.controls{display:flex;gap:8px;flex-wrap:wrap}button,select,input{background:#0b2541;border:1px solid #255178;color:#eef8ff;padding:11px;border-radius:10px;font:inherit}button{cursor:pointer}.green{background:#086a47;border-color:#0bd68d}.tabs button{font-weight:650}.tabs button.active{border-color:#23c4ff;box-shadow:0 0 10px #136b91}.hidden{display:none!important}.title{font-size:19px;font-weight:800;color:#f5c35e}.hero{text-align:center;padding:20px;border:1px solid #ad8739;border-radius:15px;background:linear-gradient(135deg,#071b32,#0b1c2b)}.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.card{border:1px solid #178b8d;background:#061e2d;border-radius:15px;padding:17px 9px;text-align:center;min-height:145px}.card:nth-child(2){border-color:#277cd2}.card:nth-child(3){border-color:#8643bd}.big{font-size:29px;font-weight:900;margin:15px 0;color:#f9d46b}.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.stat{background:#08233d;padding:16px;text-align:center;border:1px solid #215079;border-radius:12px}.stat b{font-size:28px;color:#3de3b3}.warn{color:#ffba77}.bad{color:#ff7070}.good{color:#42e6ac}.muted{color:#9ab0c3}input{width:100%;margin:5px 0 12px;background:#dfefff;color:#182c3b}.full{width:100%}.entry{border-bottom:1px solid #29445b;padding:12px 0}pre{white-space:pre-wrap;overflow-wrap:anywhere}#msg{margin-top:10px} @media(max-width:500px){.cards{gap:6px}.card{padding:12px 5px}.big{font-size:23px}.tabs button{padding:9px}}\n</style></head><body><header><h1>🌌 MEGA IA 🇧🇷</h1><div class="sub">ANÁLISE EM TEMPO REAL • HORÁRIO DE BRASÍLIA</div><div id="clock"></div></header><main>\n<section class="panel"><div class="row"><div><b>⚡ CONTROLE PRINCIPAL</b><div class="sub">App parceiro • somente Mega Bot</div></div><button id="onoff" class="green" onclick="toggleApp()">🟢 APP LIGADO</button></div></section>\n<section class="panel"><div class="controls"><select id="symbol" aria-label="Ativo IQ Option"><option>EUR/USD</option><option>GBP/USD</option><option>EUR/JPY</option><option>USD/JPY</option><option>GBP/JPY</option><option>AUD/USD</option><option>USD/CAD</option></select><select disabled><option>1min</option></select><select id="assetGroup" onchange="renderAssets()"><option value="ALL">Todos os mercados</option><option value="FOREX_OPEN">Forex aberto</option><option value="FOREX_OTC">Forex OTC</option><option value="CRYPTO_OPEN">Cripto aberto</option><option value="CRYPTO_OTC">Cripto OTC</option><option value="OTHER_OTC">Outros OTC</option></select><button onclick="loadAssets(true)">↻ Atualizar ativos IQ</button><button>🟢 Mega Bot • AUTOMÁTICO</button></div><div class="sub" style="margin-top:9px">Mercado aberto e OTC • candles IQ Option • entrada na próxima vela • pré-alerta 20s</div></section>\n<div class="sub" id="assetStatus" role="status">Conecte à IQ para carregar os ativos.</div><p style="text-align:center;font-weight:700">App criado e desenvolvido por Ismael Trader</p>\n<div class="controls tabs"><button class="active" onclick="tab(\'painel\',this)">📊 Painel</button><button onclick="tab(\'resultados\',this)">🎯 Resultados</button><button onclick="tab(\'historico\',this)">🗓 Histórico 15 dias</button><button onclick="tab(\'gestao\',this)">⚙️ Gestão</button><button onclick="tab(\'corretora\',this)">🏦 Corretora</button></div>\n<section id="painel"><div class="hero"><div class="title">🏆 MEGA BOT</div><p>ANÁLISE EM TEMPO REAL • ENTRADA NA PRÓXIMA VELA</p><div class="sub">SCALPING M1 FLEX • M15 / M5 / M1</div></div><div class="cards" style="margin-top:10px"><div class="card"><b>🤖 DIREÇÃO DO SINAL</b><div class="big" id="direction">NEUTRO</div><small id="confidence">Confiança: 0%</small></div><div class="card"><b>◷ CRONOGRAMA</b><div class="big" id="timer">--:--</div><small>Entrada: próxima M1</small></div><div class="card"><b>⚡ STATUS DO MOTOR</b><div class="big" id="engine">AGUARDANDO</div><small id="source">Fonte: nenhuma</small></div></div><div class="panel"><b>📡 FONTE DE DADOS</b><p id="reason" class="warn">Conecte a IQ Option na aba Corretora.</p></div><div class="panel"><b>🔭 Radar de oportunidades</b><p id="radar">⚪ EUR/USD • aguardando dados IQ</p><div class="sub">Pré-alerta informativo; não executa ordens na corretora.</div></div></section>\n<section id="resultados" class="hidden"><div class="panel"><h3>📊 RESUMO GERAL</h3><div class="stats"><div class="stat">WIN<br><b id="wins">0</b></div><div class="stat">LOSS FINAL<br><b id="losses">0</b></div><div class="stat">ACERTO<br><b id="accuracy">0%</b></div></div><p class="sub">Resultados virtuais pelas velas IQ. Não são operações executadas.</p></div><div class="panel"><h3>⏳ Sequência em andamento</h3><pre id="active">Nenhuma</pre></div></section>\n<section id="historico" class="hidden"><div class="panel"><h3>🗓 Histórico de resultados</h3><div id="history">Sem operações encerradas.</div><p class="sub">Histórico mantido na memória da sessão; reinício do servidor apaga os dados deste protótipo.</p></div></section>\n<section id="gestao" class="hidden"><div class="panel"><h3>⚙️ Gestão</h3><p><b>Recuperação no próximo sinal</b></p><p>LOSS inicial → aguardar próximo sinal confirmado (REC 1) → se perder, aguardar outro sinal (REC 2) → somente então LOSS FINAL.</p><p class="sub">Gale automático na vela seguinte não está ativado nesta primeira versão.</p></div></section>\n<section id="corretora" class="hidden"><div class="panel"><h3>🏦 Corretora</h3><select disabled class="full"><option>IQ Option • fonte exclusiva de dados</option></select><p id="iqstate" class="warn">🔴 IQ Option desconectada</p><label>E-mail da corretora<input id="email" type="email" autocomplete="username" placeholder="Seu e-mail IQ"></label><label>Senha<input id="password" type="password" autocomplete="current-password" placeholder="Sua senha IQ"></label><button class="full green" onclick="login()">🔐 CONECTAR IQ OPTION</button><button class="full" style="margin-top:10px" onclick="logout()">DESCONECTAR</button><div id="msg" class="warn"></div><p class="sub">A senha é enviada ao servidor somente no login; não é salva no navegador. Este app não executa ordens.</p></div><div class="panel"><b>🔬 Teste de candles IQ</b><p>Verifica 75+ velas M1 fechadas, atualizadas e sem lacunas recentes.</p><button onclick="testCandles()">TESTAR CANDLES M1</button><pre id="testout"></pre></div></section>\n</main><script>\nlet enabled=true;let busy=false;const $=id=>document.getElementById(id);function tab(id,btn){[\'painel\',\'resultados\',\'historico\',\'gestao\',\'corretora\'].forEach(x=>$(x).classList.toggle(\'hidden\',x!==id));document.querySelectorAll(\'.tabs button\').forEach(x=>x.classList.remove(\'active\'));btn.classList.add(\'active\')}\nfunction toggleApp(){enabled=!enabled;$(\'onoff\').textContent=enabled?\'🟢 APP LIGADO\':\'🔴 APP DESLIGADO\';$(\'onoff\').classList.toggle(\'green\',enabled)}\nasync function api(path,opts={}){let r=await fetch(path,{credentials:\'same-origin\',...opts});let j=await r.json();if(!r.ok)throw Error(j.detail||\'Falha de comunicação\');return j}\nlet iqAssets=[],assetsLoading=false,lastAssets=0;\nconst groupNames={FOREX_OPEN:\'Forex • aberto\',FOREX_OTC:\'Forex • OTC\',CRYPTO_OPEN:\'Cripto • aberto\',CRYPTO_OTC:\'Cripto • OTC\',OTHER_OTC:\'Outros • OTC\'};\nfunction renderAssets(){let old=$(\'symbol\').value;let filter=$(\'assetGroup\').value;let list=iqAssets.filter(x=>filter===\'ALL\'||x.group===filter);if(!iqAssets.length)return;$(\'symbol\').replaceChildren();for(let [group,title] of Object.entries(groupNames)){let subset=list.filter(x=>x.group===group);if(!subset.length)continue;let optgroup=document.createElement(\'optgroup\');optgroup.label=title;for(let a of subset){let option=document.createElement(\'option\');option.value=a.symbol;option.textContent=a.label+(a.open===false?\' • fechado\':\'\');optgroup.appendChild(option)}$(\'symbol\').appendChild(optgroup)}if(list.some(a=>a.symbol===old))$(\'symbol\').value=old;if(!list.length){let o=document.createElement(\'option\');o.value=\'\';o.textContent=\'Nenhum ativo IQ nesta categoria\';$(\'symbol\').appendChild(o)}$(\'direction\').textContent=\'NEUTRO\';$(\'confidence\').textContent=\'Confiança: 0%\'}\nasync function loadAssets(force=false){if(assetsLoading)return;assetsLoading=true;try{let d=await api(\'/iq-assets\'+(force?\'?refresh=true\':\'\'));iqAssets=d.assets||[];renderAssets();$(\'assetStatus\').textContent=d.count+\' ativos IQ • \'+(d.loading?\'atualizando lista...\':d.errors.length?\'lista parcial; algumas consultas não responderam\':\'lista atualizada\')+\' • velas verificadas por ativo\';lastAssets=Date.now();if(d.loading)setTimeout(()=>loadAssets(),3000)}catch(e){$(\'assetStatus\').textContent=e.message}finally{assetsLoading=false}}\n\nasync function login(){let e=$(\'email\').value,p=$(\'password\').value;$(\'msg\').textContent=\'Conectando à IQ Option...\';try{let r=await api(\'/iq-login\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({email:e,password:p})});$(\'password\').value=\'\';$(\'msg\').textContent=r.message;await status();await loadAssets(true)}catch(err){$(\'password\').value=\'\';$(\'msg\').textContent=err.message}}\nasync function logout(){try{await api(\'/iq-logout\',{method:\'POST\'});await status()}catch(e){$(\'msg\').textContent=e.message}}\nasync function status(){try{let d=await api(\'/iq-status\');$(\'iqstate\').textContent=d.connected?\'🟢 IQ Option conectada\':\'🔴 IQ Option desconectada\';return d.connected}catch(e){return false}}\nasync function testCandles(){$(\'testout\').textContent=\'Consultando IQ...\';try{if(!$(\'symbol\').value)throw Error(\'Selecione um ativo IQ\');let d=await api(\'/candles?symbol=\'+encodeURIComponent($(\'symbol\').value)+\'&limit=5\');$(\'testout\').textContent=JSON.stringify(d,null,2)}catch(e){$(\'testout\').textContent=e.message}}\nasync function poll(){if(!enabled||busy)return;busy=true;try{let connected=await status();if(!connected){$(\'engine\').textContent=\'OFFLINE\';$(\'reason\').textContent=\'AGUARDANDO DADOS • IQ Option desconectada\';$(\'direction\').textContent=\'NEUTRO\';return}if(Date.now()-lastAssets>300000)loadAssets();let symbol=$(\'symbol\').value;if(!symbol){$(\'reason\').textContent=\'Nenhum ativo nesta categoria\';return}let d=await api(\'/signal-ai?symbol=\'+encodeURIComponent(symbol));$(\'direction\').textContent=d.direction||\'NEUTRO\';$(\'confidence\').textContent=\'Confiança: \'+(d.confidence||0)+\'%\';$(\'engine\').textContent=d.ok?\'ONLINE\':\'AGUARDANDO\';$(\'source\').textContent=\'Fonte: \'+(d.source||\'NONE\');$(\'reason\').textContent=d.reason||\'\';$(\'radar\').textContent=symbol+\' • \'+(d.direction||\'NEUTRO\')+\' • \'+(d.reason||\'\');let p=await api(\'/performance\');$(\'wins\').textContent=p.wins;$(\'losses\').textContent=p.losses;$(\'accuracy\').textContent=p.accuracy+\'%\';$(\'active\').textContent=JSON.stringify(p.active,null,2);$(\'history\').innerHTML=p.history.length?p.history.slice().reverse().map(x=>\'<div class="entry">\'+new Date(x.finished_at*1000).toLocaleString(\'pt-BR\')+\' • \'+x.symbol+\' • <b>\'+x.final+\'</b><div class="sub">\'+x.steps.map(y=>[\'INICIAL\',\'REC 1\',\'REC 2\'][y.stage]+\': \'+y.result).join(\' → \')+\'</div></div>\').join(\'\'):\'Sem operações encerradas.\'}catch(e){$(\'reason\').textContent=e.message}finally{busy=false}}\nfunction tick(){let n=new Date();$(\'clock\').textContent=new Intl.DateTimeFormat(\'pt-BR\',{timeZone:\'America/Sao_Paulo\',hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\'}).format(n)+\' • Brasília\';$(\'timer\').textContent=String(60-n.getUTCSeconds()).padStart(2,\'0\')+\'s\'}setInterval(tick,1000);tick();setInterval(poll,8000);poll();status().then(connected=>{if(connected)loadAssets()});\n</script></body></html>\n'
