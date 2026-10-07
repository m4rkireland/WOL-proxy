import unittest
from unittest.mock import Mock
try:
    import wol_proxy as proxy
except ModuleNotFoundError:
    proxy=None

MAC='28:00:af:00:1a:df'
class RelayTests(unittest.TestCase):
    def test_valid_command_sends_only_to_configured_broadcast(self):
        self.assertIsNotNone(proxy,'Import-safe relay is not implemented')
        sender=Mock()
        config=proxy.Config(topic_prefix='wol/work-laptop',broadcast='192.168.60.255',allowed_macs=(MAC,))
        relay=proxy.Relay(config,sender=sender,clock=lambda:100.0)
        self.assertEqual(relay.handle('wol/work-laptop/command',MAC.encode(),retained=False),'sent')
        sender.assert_called_once_with(MAC,host='192.168.60.255',port=9,interface=None)

    def test_rejected_commands_never_reach_sender(self):
        for topic,payload,retained in [('wol/work-laptop/command',MAC.encode(),True),('wrong/command',MAC.encode(),False),('wol/work-laptop/command',b'00:11:22:33:44:55',False),('wol/work-laptop/command',b'bad',False),('wol/work-laptop/command',b'\xff',False),('wol/work-laptop/command',b'0'*128,False),('wol/work-laptop/command',(MAC+'\n').encode(),False)]:
            with self.subTest(topic=topic,payload=payload,retained=retained):
                sender=Mock(side_effect=AssertionError('External boundary reached'))
                relay=proxy.Relay(proxy.Config(topic_prefix='wol/work-laptop',allowed_macs=(MAC,)),sender=sender)
                self.assertNotEqual(relay.handle(topic,payload,retained=retained),'sent')
                sender.assert_not_called()

    def test_cooldown_serializes_concurrent_commands(self):
        from concurrent.futures import ThreadPoolExecutor
        sender=Mock();relay=proxy.Relay(proxy.Config(allowed_macs=(MAC,)),sender=sender,clock=lambda:100.0)
        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes=list(pool.map(lambda _:relay.handle('WOL-proxy/command',MAC.encode()),range(8)))
        self.assertEqual(outcomes.count('sent'),1);self.assertEqual(outcomes.count('cooldown'),7)
        sender.assert_called_once()

    def test_sender_error_is_sanitized(self):
        relay=proxy.Relay(proxy.Config(),sender=Mock(side_effect=OSError('SECRET_SENTINEL')),clock=lambda:100.0)
        result=relay.handle('WOL-proxy/command',MAC.encode())
        self.assertEqual(result,'send_failed');self.assertNotIn('SECRET_SENTINEL',result)

    def test_env_validation_and_secret_file(self):
        import tempfile,pathlib
        with tempfile.TemporaryDirectory() as folder:
            secret=pathlib.Path(folder)/'secret';secret.write_text('SECRET_SENTINEL\n')
            env={'MQTT_BROKER_HOST':'ha.markireland.me','MQTT_PASSWORD_FILE':str(secret),'MQTT_TOPIC_PREFIX':'wol/work-laptop','WOL_ALLOWED_MACS':MAC,'WOL_BROADCAST_ADDR':'192.168.60.255','WOL_SOURCE_INTERFACE':'eth0','WOL_SOURCE_SUBNET':'192.168.60.0/24','MQTT_TLS':'true'}
            cfg=proxy.Config.from_env(env)
            self.assertEqual(cfg.password,'SECRET_SENTINEL');self.assertNotIn('SECRET_SENTINEL',repr(cfg));self.assertTrue(cfg.tls)
            for key,value in [('MQTT_QOS','3'),('MQTT_BROKER_PORT','0'),('MQTT_TLS','perhaps'),('MQTT_TOPIC_PREFIX','wol/+/command'),('WOL_BROADCAST_ADDR','example.com'),('WOL_ALLOWED_MACS','invalid'),('WOL_SOURCE_SUBNET','invalid')]:
                with self.subTest(key=key),self.assertRaises(ValueError):proxy.Config.from_env(env|{key:value})

    def test_network_scope_checked_before_send(self):
        sender=Mock();cfg=proxy.Config.from_env({'WOL_SOURCE_INTERFACE':'eth0','WOL_SOURCE_SUBNET':'192.168.60.0/24','WOL_BROADCAST_ADDR':'192.168.60.255'})
        relay=proxy.Relay(cfg,sender=sender,source_ip=lambda iface:'192.168.100.145')
        self.assertEqual(relay.handle('WOL-proxy/command',MAC.encode()),'network_error');sender.assert_not_called()
        relay=proxy.Relay(cfg,sender=sender,source_ip=lambda iface:'192.168.60.2')
        self.assertEqual(relay.handle('WOL-proxy/command',MAC.encode()),'sent')
        sender.assert_called_once_with(MAC,host='192.168.60.255',port=9,interface='192.168.60.2')

if __name__=='__main__':unittest.main()
