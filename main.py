"""MEGA IA Parceiro 2.0 - projeto novo, motor Scalping M1 FLEX."""
import os, time, secrets, threading, logging
from datetime import datetime, timezone
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
SESSION_TTL = 12 * 3600
LOGIN_TIMEOUT = max(15, min(60, int(os.getenv("IQ_CONNECT_TIMEOUT", "35"))))
IQ_RECONNECT_LOCK = threading.RLock()
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
    """Executa chamadas potencialmente travadas em thread daemon; não esgota um pool fixo."""
    done = threading.Event()
    result = {}
    def runner():
        try:
            result["value"] = fn(*args)
        except BaseException as exc:
            result["error"] = exc
        finally:
            done.set()
    threading.Thread(target=runner, daemon=True, name="mega-iq-call").start()
    if not done.wait(max(0.1, float(seconds))):
        raise TimeoutError(f"A chamada {getattr(fn, '__name__', 'IQ')} excedeu {seconds}s")
    if "error" in result:
        raise result["error"]
    return result.get("value")

def login_blocking(email,password):
    iq=IQ_Option(email,password)
    ok,reason=iq.connect()
    if not ok:raise RuntimeError("IQ Option recusou ou não concluiu a autenticação: "+str(reason)[:130])
    if not iq.check_connect():raise RuntimeError("IQ Option não confirmou o WebSocket.")
    return iq

@app.post('/iq-login')
def iq_login(body: Login):
    if IQ_Option is None:raise HTTPException(503,"Biblioteca iqoptionapi não instalada no servidor.")
    try:
        iq=call_timeout(login_blocking,body.email,body.password,seconds=LOGIN_TIMEOUT)
    except TimeoutError:
        LOG.warning("[IQ CONNECTOR] timeout de %ss na autenticação/WebSocket",LOGIN_TIMEOUT)
        raise HTTPException(504,f"IQ Option não respondeu em {LOGIN_TIMEOUT}s. O servidor poderá tentar novamente; confira a rede e os logs.")
    except Exception as exc:
        LOG.warning("[IQ CONNECTOR] login falhou: %s",type(exc).__name__)
        raise HTTPException(502,"Falha na conexão IQ: "+str(exc)[:170])
    token=secrets.token_urlsafe(40)
    with LOCK:
        SESSIONS[token]={"iq":iq,"email":body.email,"password":body.password,"created":time.time(),"mode":"RECOVERY","last_signal":{},"active":{},"history":[],"last_poll":{},"last_error":"","reconnecting":False,"last_reconnect_attempt":0.0}
    response=JSONResponse({"connected":True,"message":"IQ Option conectada"})
    response.set_cookie("mega_iq_session",token,httponly=True,secure=True,samesite="lax",max_age=SESSION_TTL)
    return response

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

def ensure_iq_connected(s):
    """Verifica o socket e tenta restabelecer a sessão quando a conexão cai."""
    try:
        if call_timeout(s["iq"].check_connect, seconds=3):
            s["last_error"] = ""
            return s["iq"]
    except Exception:
        pass
    with IQ_RECONNECT_LOCK:
        # Outra requisição pode ter reconectado enquanto esta aguardava o lock.
        try:
            if call_timeout(s["iq"].check_connect, seconds=3):
                s["last_error"] = ""
                return s["iq"]
        except Exception:
            pass
        now = time.time()
        if s.get("reconnecting"):
            raise RuntimeError("Reconexão da IQ Option já está em andamento.")
        wait_until = float(s.get("last_reconnect_attempt", 0)) + min(30, 2 ** min(int(s.get("reconnect_failures", 0)), 5))
        if now < wait_until:
            raise RuntimeError(f"IQ Option reconectando; nova tentativa em {int(wait_until-now)+1}s.")
        s["reconnecting"] = True
        s["last_reconnect_attempt"] = now
        try:
            LOG.warning("[IQ RECONNECT] socket desconectado; iniciando nova autenticação")
            new_iq = call_timeout(login_blocking, s["email"], s["password"], seconds=LOGIN_TIMEOUT)
            old_iq = s.get("iq")
            s["iq"] = new_iq
            s["reconnect_failures"] = 0
            s["last_error"] = ""
            try:
                if old_iq is not new_iq:
                    threading.Thread(target=old_iq.close_connect, daemon=True).start()
            except Exception:
                pass
            LOG.info("[IQ RECONNECT] WebSocket restabelecido")
            return new_iq
        except Exception as exc:
            s["reconnect_failures"] = int(s.get("reconnect_failures", 0)) + 1
            s["last_error"] = str(exc)[:150]
            LOG.warning("[IQ RECONNECT] falha %s: %s", s["reconnect_failures"], s["last_error"])
            raise RuntimeError("Falha na reconexão automática da IQ Option: " + s["last_error"])
        finally:
            s["reconnecting"] = False

@app.get('/iq-status')
def iq_status(request:Request):
    s=session(request,False)
    if not s:return {"connected":False,"source":"NONE"}
    try:
        ensure_iq_connected(s)
        connected=True
    except Exception as exc:
        connected=False
        s["last_error"] = str(exc)[:150]
    return {"connected":connected,"source":"IQ_OPTION" if connected else "NONE","error":s.get("last_error", ""),"reconnecting":bool(s.get("reconnecting"))}

def fetch_candles(s,symbol,count=180):
    iq=ensure_iq_connected(s)
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
