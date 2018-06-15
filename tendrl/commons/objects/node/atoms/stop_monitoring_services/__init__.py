from tendrl.commons import objects
from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import log_utils as logger
from tendrl.commons.utils.service import Service


class StopMonitoringServices(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(StopMonitoringServices, self).__init__(*args, **kwargs)

    def run(self):
        service = "collectd"
        logger.log(
            "info",
            NS.publisher_id,
            {
                "message": "Stopping service %s on %s"
                           % (service, NS.node_context.fqdn)
            },
            job_id=self.parameters['job_id'],
            flow_id=self.parameters['flow_id'],
        )
        srv = NS.tendrl.objects.Service(service=service)
        if not srv.running:
            logger.log(
                "debug",
                NS.publisher_id,
                {
                    "message": "Service %s not running on %s"
                               % (service, NS.node_context.fqdn)
                },
                job_id=self.parameters['job_id'],
                flow_id=self.parameters['flow_id'],
            )
        else:
            stopped = Service(service_name=service,
                              publisher_id=NS.publisher_id,
                              enabled=True).stop()
            if stopped:
                logger.log(
                    "info",
                    NS.publisher_id,
                    {
                        "message": "Service %s stopped on %s" %
                                   (service, NS.node_context.fqdn)
                    },
                    job_id=self.parameters['job_id'],
                    flow_id=self.parameters['flow_id'],
                )

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
            else:
                logger.log(
                    "error",
                    NS.publisher_id,
                    {
                        "message": "Could not stop service %s on %s" %
                                   (service, NS.node_context.fqdn)
                    },
                    job_id=self.parameters['job_id'],
                    flow_id=self.parameters['flow_id'],
                )
                return False
        return True
