import json,tempfile,threading,time,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from wol_proxy import Config
try:
    import mqtt_runner as runner
except ModuleNotFoundError:
    runner=None

class RuntimeTests(unittest.TestCase):
    def runtime(self):
        self.assertIsNotNone(runner,'MQTT runtime is missing')
        client=Mock();client.subscribe.return_value=(0,42)
        client.publish.return_value=Mock(rc=runner.mqtt.MQTT_ERR_SUCCESS)
        config=Config(topic_prefix='wol/test',tls=True,username='relay',password='SECRET_SENTINEL')
        relay=Mock();relay.network_source.return_value=None
        runtime=runner.Runtime(config,client=client,relay=relay)
        return runtime,client,relay
    def callback_thread(self,callback):
        errors=[]
        def run():
            try:callback()
            except BaseException as exc:errors.append(exc)
        thread=threading.Thread(target=run,daemon=True)
        thread.start();thread.join(timeout=2)
        self.assertFalse(thread.is_alive(),'Lifecycle callback blocked by a held runtime lock')
        if errors:raise errors[0]
    def test_tls_callbacks_subscribe_ack_and_disconnect(self):
        runtime,client,relay=self.runtime()
        client.tls_set_context.assert_called_once();client.connect_async.assert_not_called()
        runtime.on_connect(client,None,None,5,None)
        self.assertFalse(runtime.ready);client.subscribe.assert_not_called()
        runtime.on_connect(client,None,None,0,None)
        self.assertFalse(runtime.ready)
        self.assertEqual(client.subscribe.call_args.args[0],'wol/test/command')
        runtime.on_subscribe(client,None,42,[0],None)
        self.assertTrue(runtime.ready)
        runtime.on_disconnect(client,None,None,0,None)
        self.assertFalse(runtime.ready)
        relay.handle.assert_not_called()
    def test_message_result_and_network_health_never_wake(self):
        runtime,client,relay=self.runtime()
        relay.handle.return_value='denied'
        message=Mock(topic='wol/test/command',payload=b'invalid',retain=True)
        runtime.on_message(client,None,message)
        relay.handle.assert_called_once_with(message.topic,message.payload,retained=True)
        result=json.loads(client.publish.call_args.args[1]);self.assertEqual(result,{'status':'denied'})
        relay.handle.reset_mock()
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'health.json'
            runtime.tick(path);self.assertFalse(runner.healthcheck(path))
            runtime.on_connect(client,None,None,0,None);runtime.on_subscribe(client,None,42,[0],None)
            runtime.tick(path);self.assertTrue(runner.healthcheck(path))
            relay.network_source.side_effect=OSError('SECRET_SENTINEL')
            runtime.tick(path);self.assertFalse(runner.healthcheck(path))
            self.assertNotIn('SECRET_SENTINEL',path.read_text())
        relay.handle.assert_not_called()
    def test_reconnect_during_publish_cannot_restore_old_status_cache(self):
        runtime,client,relay=self.runtime()
        runtime.on_connect(client,None,None,0,None)
        runtime.on_subscribe(client,None,42,[0],None)
        def reconnect(*args,**kwargs):
            runtime.on_disconnect(client,None,None,0,None)
            runtime.on_connect(client,None,None,0,None)
            runtime.on_subscribe(client,None,42,[0],None)
            return Mock(rc=runner.mqtt.MQTT_ERR_SUCCESS)
        client.publish.side_effect=reconnect
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'health.json'
            runtime.tick(path)
            self.assertTrue(runtime.ready)
            self.assertIsNone(runtime.last_status)
            client.publish.side_effect=None
            runtime.tick(path)
        self.assertEqual(client.publish.call_count,2)
        self.assertEqual(runtime.last_status,'Online')
        relay.handle.assert_not_called()
    def test_lifecycle_change_during_network_inspection_invalidates_health(self):
        for reconnect in (False,True):
            with self.subTest(reconnect=reconnect):
                runtime,client,relay=self.runtime()
                runtime.on_connect(client,None,None,0,None)
                runtime.on_subscribe(client,None,42,[0],None)
                def inspect():
                    runtime.on_disconnect(client,None,None,0,None)
                    if reconnect:
                        runtime.on_connect(client,None,None,0,None)
                        runtime.on_subscribe(client,None,42,[0],None)
                relay.network_source.side_effect=inspect
                with tempfile.TemporaryDirectory() as folder:
                    path=Path(folder)/'health.json'
                    self.assertFalse(runtime.tick(path))
                    self.assertFalse(runner.healthcheck(path))
                    client.publish.assert_not_called()
                    self.assertIsNone(runtime.last_status)
                    relay.network_source.side_effect=None
                    self.assertEqual(runtime.tick(path),reconnect)
                relay.handle.assert_not_called()
    def test_failed_status_publication_is_retried_not_cached(self):
        runtime,client,relay=self.runtime()
        runtime.on_connect(client,None,None,0,None)
        runtime.on_subscribe(client,None,42,[0],None)
        client.publish.side_effect=[Mock(rc=runner.mqtt.MQTT_ERR_NO_CONN),
                                    Mock(rc=runner.mqtt.MQTT_ERR_SUCCESS)]
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'health.json'
            runtime.tick(path)
            self.assertIsNone(runtime.last_status)
            runtime.tick(path)
        self.assertEqual(client.publish.call_count,2)
        self.assertEqual(runtime.last_status,'Online')
        relay.handle.assert_not_called()
    def test_stale_subscription_return_cannot_commit_to_new_lifecycle(self):
        for reconnect in (False,True):
            with self.subTest(reconnect=reconnect):
                runtime,client,relay=self.runtime()
                def subscribe(*args,**kwargs):
                    runtime.on_disconnect(client,None,None,0,None)
                    if reconnect:
                        client.subscribe.side_effect=None
                        client.subscribe.return_value=(0,43)
                        runtime.on_connect(client,None,None,0,None)
                    return (0,42)
                client.subscribe.side_effect=subscribe
                runtime.on_connect(client,None,None,0,None)
                self.assertEqual(runtime.subscribe_mid,43 if reconnect else None)
                runtime.on_subscribe(client,None,42,[0],None)
                self.assertFalse(runtime.subscribed)
                self.assertFalse(runtime.ready)
                if reconnect:
                    runtime.on_subscribe(client,None,43,[0],None)
                    self.assertTrue(runtime.ready)
                relay.handle.assert_not_called()
    def test_publish_callback_cannot_commit_old_generation_health(self):
        runtime,client,relay=self.runtime()
        runtime.on_connect(client,None,None,0,None)
        runtime.on_subscribe(client,None,42,[0],None)
        def disconnect(*args,**kwargs):
            runtime.on_disconnect(client,None,None,0,None)
            return Mock(rc=runner.mqtt.MQTT_ERR_SUCCESS)
        client.publish.side_effect=disconnect
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'health.json'
            self.assertFalse(runtime.tick(path))
            self.assertFalse(runner.healthcheck(path))
        self.assertIsNone(runtime.last_status)
        relay.handle.assert_not_called()
    def test_stop_invalidates_readiness_before_wait_and_ignores_late_connect(self):
        runtime,client,relay=self.runtime()
        runtime.on_connect(client,None,None,0,None)
        runtime.on_subscribe(client,None,42,[0],None)
        def waiting(**kwargs):
            self.assertFalse(runtime.ready)
            self.assertIsNone(runtime.last_status)
            runtime.on_connect(client,None,None,0,None)
            runtime.on_subscribe(client,None,42,[0],None)
            self.assertFalse(runtime.ready)
        client.publish.return_value.wait_for_publish.side_effect=waiting
        runtime.stop()
        client.publish.assert_called_once_with('wol/test/status','Offline',qos=1,retain=True)
        client.publish.return_value.wait_for_publish.assert_called_once_with(timeout=3)
        client.disconnect.assert_called_once();client.loop_stop.assert_called_once()
        client.subscribe.assert_called_once()
        relay.handle.assert_not_called()
    def test_network_inspection_does_not_hold_callback_lock(self):
        runtime,client,relay=self.runtime()
        runtime.on_connect(client,None,None,0,None)
        runtime.on_subscribe(client,None,42,[0],None)
        relay.network_source.side_effect=lambda:self.callback_thread(
            lambda:runtime.on_disconnect(client,None,None,0,None))
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'health.json'
            self.assertFalse(runtime.tick(path))
            self.assertFalse(runner.healthcheck(path))
        client.publish.assert_not_called();relay.handle.assert_not_called()
    def test_stop_wait_and_network_join_do_not_hold_callback_lock(self):
        runtime,client,relay=self.runtime()
        runtime.on_connect(client,None,None,0,None)
        runtime.on_subscribe(client,None,42,[0],None)
        def callback(*args,**kwargs):
            self.callback_thread(lambda:runtime.on_disconnect(client,None,None,0,None))
        client.publish.return_value.wait_for_publish.side_effect=callback
        client.disconnect.side_effect=callback
        client.loop_stop.side_effect=callback
        runtime.stop()
        client.publish.return_value.wait_for_publish.assert_called_once_with(timeout=3)
        client.disconnect.assert_called_once();client.loop_stop.assert_called_once()
        self.assertFalse(runtime.ready);relay.handle.assert_not_called()
    def test_missing_corrupt_stale_health_is_unhealthy(self):
        self.assertIsNotNone(runner)
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'health.json';self.assertFalse(runner.healthcheck(path))
            path.write_text('bad');self.assertFalse(runner.healthcheck(path))
            path.write_text(json.dumps({'healthy':True,'timestamp':time.monotonic()-100}))
            self.assertFalse(runner.healthcheck(path))

if __name__=='__main__':unittest.main()
