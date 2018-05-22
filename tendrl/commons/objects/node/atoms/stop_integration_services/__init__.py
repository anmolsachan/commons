from tendrl.commons import objects
from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import log_utils as logger


class StopIntegrationServices(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(StopIntegrationServices, self).__init__(*args, **kwargs)

    def run(self):
        services = ["tendrl-gluster-integration"]
        for service in services:
            srv = NS.tendrl.objects.Service(service=service)
            if not srv.running:
                logger.log(
                    "debug",
                    NS.publisher_id,
                    {
                        "message": "%s not running on "
                        "%s" % (service, NS.node_context.fqdn)
                    },
                    job_id=self.parameters['job_id'],
                    flow_id=self.parameters['flow_id'],
                )
                continue

            _cmd_str = "systemctl stop %s" % service
            cmd = cmd_utils.Command(_cmd_str)
            err, out, rc = cmd.run()
            if err:
                logger.log(
                    "error",
                    NS.publisher_id,
                    {
                        "message": "Could not stop %s"
                        " service. Error: %s" % (service, err)
                    },
                    job_id=self.parameters['job_id'],
                    flow_id=self.parameters['flow_id'],
                )
                return False

            _cmd_str = "systemctl disable %s" % service
            cmd = cmd_utils.Command(_cmd_str)
            err, out, rc = cmd.run()
            if err:
                logger.log(
                    "error",
                    NS.publisher_id,
                    {
                        "message": "Could not disable %s"
                        " service. Error: %s" % (service, err)
                    },
                    job_id=self.parameters['job_id'],
                    flow_id=self.parameters['flow_id'],
                )
                return False
        return True
