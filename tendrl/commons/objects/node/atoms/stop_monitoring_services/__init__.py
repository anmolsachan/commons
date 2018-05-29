from tendrl.commons.flows import service_utils
from tendrl.commons import objects


class StopMonitoringServices(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(StopMonitoringServices, self).__init__(*args, **kwargs)

    def run(self):
        return service_utils.stop_service(
            ["collectd"],
            self.parameters
        )
