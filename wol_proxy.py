"""Import-safe MQTT-to-WoL relay. Derived from seanauff/WOL-proxy (MIT)."""
from dataclasses import dataclass, field
from pathlib import Path
import os
import ipaddress
import socket
import struct
import fcntl
import time
import re
import threading
from wakeonlan import wake

def normalize_mac(value):
    if not isinstance(value,str) or not re.fullmatch(r'[0-9a-fA-F]{2}([-:.]?)[0-9a-fA-F]{2}(\1[0-9a-fA-F]{2}){4}',value):
        raise ValueError('Invalid MAC address')
    raw=re.sub(r'[-:.]','',value).lower()
    return ':'.join(raw[i:i+2] for i in range(0,12,2))

@dataclass(frozen=True)
class Config:
    topic_prefix:str='WOL-proxy'
    broadcast:str='255.255.255.255'
    allowed_macs:tuple[str,...]=()
    source_ip:str|None=None
    port:int=9
    broker:str='127.0.0.1'
    broker_port:int=1883
    client_id:str='WOL-proxy'
    username:str=''
    password:str=field(default='',repr=False)
    qos:int=1
    tls:bool=False
    ca_file:str|None=None
    source_interface:str|None=None
    source_subnet:str|None=None

    @classmethod
    def from_env(cls,env=None):
        env=os.environ if env is None else env
        def integer(key,default,low,high):
            try:value=int(env.get(key,str(default)))
            except (TypeError,ValueError):raise ValueError('Invalid '+key) from None
            if not low<=value<=high:raise ValueError('Invalid '+key)
            return value
        tls=env.get('MQTT_TLS','false').lower()
        if tls not in ('true','false'):raise ValueError('Invalid MQTT_TLS')
        prefix=env.get('MQTT_TOPIC_PREFIX','WOL-proxy')
        if not prefix or len(prefix)>200 or any(c in prefix for c in ('+','#','\x00')):raise ValueError('Invalid MQTT_TOPIC_PREFIX')
        broadcast=env.get('WOL_BROADCAST_ADDR','255.255.255.255')
        ipaddress.IPv4Address(broadcast)
        subnet=env.get('WOL_SOURCE_SUBNET') or None
        if subnet:
            network=ipaddress.IPv4Network(subnet)
            if str(network.broadcast_address)!=broadcast:raise ValueError('Broadcast does not match source subnet')
        source=env.get('WOL_SOURCE_IP') or None
        if source:ipaddress.IPv4Address(source)
        iface=env.get('WOL_SOURCE_INTERFACE') or None
        if iface and not re.fullmatch(r'[a-zA-Z0-9_.-]{1,15}',iface):raise ValueError('Invalid WOL_SOURCE_INTERFACE')
        if subnet and not (source or iface):raise ValueError('Source subnet requires source interface/IP')
        allow=env.get('WOL_ALLOWED_MACS','')
        allowed=tuple(normalize_mac(x) for x in allow.split(',')) if allow else ()
        secret=env.get('MQTT_PASSWORD','')
        if env.get('MQTT_PASSWORD_FILE'):
            secret=Path(env['MQTT_PASSWORD_FILE']).read_text().rstrip('\r\n')
        return cls(topic_prefix=prefix,broadcast=broadcast,allowed_macs=allowed,source_ip=source,
                   port=integer('WOL_PORT',9,1,65535),broker=env.get('MQTT_BROKER_HOST','127.0.0.1'),
                   broker_port=integer('MQTT_BROKER_PORT',8883 if tls=='true' else 1883,1,65535),
                   client_id=env.get('MQTT_CLIENT_ID','WOL-proxy'),username=env.get('MQTT_USERNAME',''),
                   password=secret,qos=integer('MQTT_QOS',1,0,2),tls=tls=='true',
                   ca_file=env.get('MQTT_TLS_CA_FILE') or None,source_interface=iface,source_subnet=subnet)

def interface_ip(iface):
    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as sock:
        result=fcntl.ioctl(sock.fileno(),0x8915,struct.pack('256s',iface.encode()))
    return socket.inet_ntoa(result[20:24])

class Relay:
    def __init__(self,config,*,sender=wake,clock=time.monotonic,source_ip=interface_ip):
        self.config=config;self.sender=sender;self.clock=clock
        self.lock=threading.Lock();self.last_send=None;self.source_ip=source_ip
    def network_source(self):
        source=self.config.source_ip
        if self.config.source_interface:source=self.source_ip(self.config.source_interface)
        if self.config.source_subnet:
            address=ipaddress.IPv4Address(source);network=ipaddress.IPv4Network(self.config.source_subnet)
            if address not in network or address in (network.network_address,network.broadcast_address):raise ValueError('Wrong source network')
        return source
    def handle(self,topic,payload,*,retained=False):
        if retained or topic!=self.config.topic_prefix+'/command':return 'ignored'
        if not isinstance(payload,bytes) or len(payload)>32:return 'invalid'
        try:mac=normalize_mac(payload.decode('ascii'))
        except (UnicodeDecodeError,ValueError):return 'invalid'
        if self.config.allowed_macs and mac not in self.config.allowed_macs:return 'denied'
        with self.lock:
            now=self.clock()
            if self.last_send is not None and now-self.last_send<3:return 'cooldown'
            try:source=self.network_source()
            except (OSError,ValueError,TypeError):return 'network_error'
            self.last_send=now
            try:self.sender(mac,host=self.config.broadcast,port=self.config.port,interface=source)
            except OSError:return 'send_failed'
            return 'sent'
