"""Client for the optional broker-backed Registry service."""
from .client import SRPCClient


class RegistryClient(SRPCClient):
    def __init__(self, broker_addr=None, timeo=1, service_name="Registry", **kwargs):
        super().__init__(broker_addr=broker_addr, timeo=timeo, **kwargs)
        self.service_name = service_name

    def heartbeat(self, info):
        return self.invoke(self.service_name, "heartbeat", kwargs={"info": info})

    def services(self):
        return self.invoke(self.service_name, "services")
