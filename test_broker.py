"""Real local broker test. The packet sender is mocked: no WoL is emitted."""
import json,shutil,socket,subprocess,tempfile,time,unittest
from pathlib import Path
from unittest.mock import Mock
import paho.mqtt.client as mqtt
from wol_proxy import Config,Relay
from mqtt_runner import Runtime

@unittest.skipUnless(shutil.which('mosquitto'),'Local Mosquitto not installed')
class BrokerTests(unittest.TestCase):
    def test_failed_availability_publish_is_not_replayed_on_reconnect(self):
        with tempfile.TemporaryDirectory() as folder:
            with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            cfg=Path(folder)/'mosquitto.conf';cfg.write_text(f'listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n')
            broker=subprocess.Popen(['mosquitto','-c',str(cfg)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            observer=None;runtime=None
            try:
                def until(predicate,pump=lambda:None):
                    deadline=time.monotonic()+8
                    while time.monotonic()<deadline:
                        pump()
                        if predicate():return
                        time.sleep(.01)
                    self.fail('Timed out waiting for local broker condition')
                def listening():
                    try:
                        with socket.create_connection(('127.0.0.1',port),timeout=.1):return True
                    except OSError:return False
                until(listening)
                prefix='test/availability';statuses=[];barriers=[];subscribed=[]
                observer=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='availability-observer',protocol=mqtt.MQTTv5)
                observer.on_subscribe=lambda *args:subscribed.append(True)
                def observe(client,userdata,message):
                    if message.topic==prefix+'/status':statuses.append(message.payload.decode())
                    else:barriers.append(message.payload.decode())
                observer.on_message=observe
                observer.connect('127.0.0.1',port)
                observer.subscribe([(prefix+'/status',1),(prefix+'/barrier',1)])
                until(lambda:subscribed,lambda:observer.loop(timeout=.01))
                sender=Mock();source=Mock(return_value='192.0.2.10')
                config=Config(topic_prefix=prefix,client_id='availability-runtime',broker='127.0.0.1',broker_port=port,
                              source_interface='mock0',source_subnet='192.0.2.0/24',broadcast='192.0.2.255')
                runtime=Runtime(config,relay=Relay(config,sender=sender,source_ip=source))
                client=runtime.client
                def pump():
                    client.loop(timeout=.01);observer.loop(timeout=.01)
                client.connect('127.0.0.1',port)
                until(lambda:runtime.ready,pump)
                # Delay only callback delivery, not Paho's real disconnect processing.
                # This recreates the window where Runtime still thinks it is ready.
                disconnects=[]
                client.on_disconnect=lambda *args:disconnects.append(args)
                client.socket().shutdown(socket.SHUT_RDWR)
                until(lambda:disconnects,pump)
                until(lambda:statuses==['Offline'],lambda:observer.loop(timeout=.01))
                self.assertTrue(runtime.ready)
                self.assertIsNone(client.socket())
                real_publish=client.publish;publications=[]
                def publish(*args,**kwargs):
                    info=real_publish(*args,**kwargs)
                    publications.append((args,kwargs,info.rc))
                    return info
                client.publish=publish
                health=Path(folder)/'health.json'
                runtime.tick(health)
                attempted,options,rc=publications[-1]
                self.assertEqual(attempted,(prefix+'/status','Online'))
                self.assertTrue(options['retain'])
                self.assertEqual(rc,mqtt.MQTT_ERR_NO_CONN)
                self.assertIsNone(runtime.last_status)
                source.return_value='198.51.100.10'
                runtime.on_disconnect(*disconnects.pop())
                client.on_disconnect=runtime.on_disconnect
                client.reconnect()
                until(lambda:runtime.ready,pump)
                def barrier(label):
                    client.publish(prefix+'/barrier',label,qos=1,retain=False)
                    until(lambda:label in barriers,pump)
                barrier('reconnected')
                before_tick=list(statuses)
                self.assertIsNone(runtime.last_status)
                self.assertFalse(runtime.tick(health))
                barrier('unready-tick')
                self.assertEqual(statuses,['Offline','Offline'],
                                 f'Stale availability replay before current-generation tick: {before_tick}')
                source.return_value='192.0.2.10'
                self.assertTrue(runtime.tick(health))
                barrier('ready-tick')
                self.assertEqual(statuses,['Offline','Offline','Online'])
                sender.assert_not_called()
            finally:
                if runtime:runtime.stop()
                if observer:observer.disconnect();observer.loop_stop()
                if broker.poll() is None:broker.terminate();broker.wait(timeout=3)

    def test_retained_commands_and_real_command_result(self):
        def until(predicate):
            deadline=time.monotonic()+8
            while time.monotonic()<deadline:
                if predicate():return
                time.sleep(.05)
            self.fail('Timed out waiting for local broker condition')
        with tempfile.TemporaryDirectory() as folder:
            with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            cfg=Path(folder)/'mosquitto.conf';cfg.write_text(f'listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n')
            broker=subprocess.Popen(['mosquitto','-c',str(cfg)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            publisher=None;runtime=None
            try:
                def listening():
                    try:
                        with socket.create_connection(('127.0.0.1',port),timeout=.1):return True
                    except OSError:return False
                until(listening)
                prefix='test/wol';mac='02:00:00:00:00:01';results=[]
                publisher=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='test-publisher',protocol=mqtt.MQTTv5)
                publisher.on_message=lambda c,u,m:results.append(json.loads(m.payload))
                publisher.connect('127.0.0.1',port);publisher.loop_start()
                publisher.subscribe(prefix+'/result',qos=1)
                publisher.publish(prefix+'/command',mac,qos=1,retain=True).wait_for_publish(timeout=3)
                sender=Mock();config=Config(topic_prefix=prefix,allowed_macs=(mac,))
                runtime=Runtime(config,relay=Relay(config,sender=sender))
                runtime.client.connect_async('127.0.0.1',port,keepalive=10);runtime.client.loop_start()
                until(lambda:runtime.ready)
                time.sleep(.2);sender.assert_not_called()
                # Retained publications delivered to a live subscriber must also be rejected.
                publisher.publish(prefix+'/command',mac,qos=1,retain=True).wait_for_publish(timeout=3)
                until(lambda:len(results)==1);sender.assert_not_called()
                self.assertEqual(results.pop(),{'status':'ignored'})
                publisher.publish(prefix+'/command',mac,qos=1,retain=False).wait_for_publish(timeout=3)
                until(lambda:len(results)==1);sender.assert_called_once()
                self.assertEqual(results[0],{'status':'sent'})
                runtime.tick(Path(folder)/'health.json')
                broker.terminate();broker.wait(timeout=3)
                until(lambda:not runtime.ready)
                sender.assert_called_once()
            finally:
                if runtime:runtime.stop()
                if publisher:publisher.disconnect();publisher.loop_stop()
                if broker.poll() is None:broker.terminate();broker.wait(timeout=3)

if __name__=='__main__':unittest.main()
