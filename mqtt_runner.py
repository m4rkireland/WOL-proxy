"""MQTT v5 lifecycle, health and TLS; importing this module has no side effects."""
import json
import signal
import ssl
import sys
import threading
import time
from pathlib import Path
import paho.mqtt.client as mqtt
from wol_proxy import Config,Relay

HEALTH_PATH=Path('/tmp/wol-proxy-health.json')

def healthcheck(path=HEALTH_PATH):
    try:
        data=json.loads(Path(path).read_text())
        age=time.monotonic()-data['timestamp']
        return data['healthy'] is True and 0<=age<30
    except (OSError,ValueError,KeyError,TypeError):return False

class Runtime:
    def __init__(self,config,*,client=None,relay=None):
        self.config=config;self.relay=relay or Relay(config)
        self.client=client or mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=config.client_id,protocol=mqtt.MQTTv5)
        self.connected=False;self.subscribed=False;self.subscribe_mid=None;self.last_status=None
        self._lock=threading.RLock();self._generation=0;self._stopping=False
        client=self.client
        client.on_connect=self.on_connect;client.on_disconnect=self.on_disconnect
        client.on_message=self.on_message;client.on_subscribe=self.on_subscribe
        client.will_set(config.topic_prefix+'/status','Offline',qos=1,retain=True)
        if config.username:client.username_pw_set(config.username,config.password)
        if config.tls:client.tls_set_context(ssl.create_default_context(cafile=config.ca_file))
        client.reconnect_delay_set(min_delay=1,max_delay=30)
        client.max_inflight_messages_set(10);client.max_queued_messages_set(10)
    @property
    def ready(self):
        with self._lock:return self.connected and self.subscribed
    def on_connect(self,client,userdata,flags,reason_code,properties):
        with self._lock:
            if self._stopping:return
            self._generation+=1
            generation=self._generation
            self.connected=reason_code==0;self.subscribed=False;self.subscribe_mid=None;self.last_status=None
            if self.connected:
                status,mid=client.subscribe(self.config.topic_prefix+'/command',options=mqtt.SubscribeOptions(qos=self.config.qos,retainHandling=2,retainAsPublished=True))
                if generation==self._generation and status==mqtt.MQTT_ERR_SUCCESS:
                    self.subscribe_mid=mid
    def on_subscribe(self,client,userdata,mid,reason_codes,properties):
        with self._lock:
            if self.connected and self.subscribe_mid is not None and mid==self.subscribe_mid:
                self.subscribed=bool(reason_codes) and all(int(getattr(r,'value',r))<128 for r in reason_codes)
    def on_disconnect(self,client,userdata,flags,reason_code,properties):
        with self._lock:
            self._generation+=1
            self.connected=False;self.subscribed=False;self.subscribe_mid=None;self.last_status=None
    def on_message(self,client,userdata,message):
        outcome=self.relay.handle(message.topic,message.payload,retained=message.retain)
        client.publish(self.config.topic_prefix+'/result',json.dumps({'status':outcome}),qos=0,retain=False)
        print(json.dumps({'event':'command','status':outcome}),flush=True)
    def tick(self,path=HEALTH_PATH):
        with self._lock:
            generation=self._generation
            healthy=self.ready
        try:self.relay.network_source()
        except (OSError,ValueError,TypeError):healthy=False
        with self._lock:
            healthy=healthy and self.ready and generation==self._generation
            status='Online' if healthy else 'Offline'
            if generation==self._generation and self.connected and status!=self.last_status:
                # QoS 1 is queued by Paho even on MQTT_ERR_NO_CONN and replayed
                # after reconnect. Readiness must be recomputed, not replayed.
                info=self.client.publish(self.config.topic_prefix+'/status',status,qos=0,retain=True)
                # A mocked publish can call lifecycle callbacks reentrantly.
                if generation==self._generation and info.rc==mqtt.MQTT_ERR_SUCCESS:
                    self.last_status=status
            healthy=healthy and self.ready and generation==self._generation
            path=Path(path);tmp=path.with_suffix('.tmp')
            tmp.write_text(json.dumps({'healthy':healthy,'timestamp':time.monotonic()}));tmp.replace(path)
            return healthy
    def stop(self):
        with self._lock:
            connected=self.connected
            self._stopping=True;self._generation+=1
            self.connected=False;self.subscribed=False;self.subscribe_mid=None;self.last_status=None
            info=self.client.publish(self.config.topic_prefix+'/status','Offline',qos=1,retain=True) if connected else None
        # These operations can wait for the network thread, whose callbacks need the lock.
        if info is not None:
            try:info.wait_for_publish(timeout=3)
            except (ValueError,RuntimeError):pass
        self.client.disconnect();self.client.loop_stop()

def main():
    if '--healthcheck' in sys.argv:return 0 if healthcheck() else 1
    try:runtime=Runtime(Config.from_env())
    except (ValueError,OSError):
        print('Invalid relay configuration or TLS certificate file',file=sys.stderr);return 2
    stop=threading.Event()
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
    runtime.client.connect_async(runtime.config.broker,runtime.config.broker_port,keepalive=30)
    runtime.client.loop_start()
    print('MQTT relay starting; health checks never send wake packets',flush=True)
    try:
        while not stop.wait(5):runtime.tick()
    finally:runtime.stop()
    return 0

if __name__=='__main__':raise SystemExit(main())
