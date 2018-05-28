from tendrl.commons.flows import service_utils
from tendrl.commons import objects


class StopIntegrationServices(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(StopIntegrationServices, self).__init__(*args, **kwargs)

    def run(self):
        services = ["tendrl-gluster-integration"]
        return service_utils.stop_service(services, self.parameters)
