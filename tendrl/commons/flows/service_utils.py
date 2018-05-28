""" This utility can be used to handle(start, stop, etc.) tendrl services.
"""

from tendrl.commons.utils import cmd_utils
from tendrl.commons.utils import log_utils as logger


def stop_service(services, params):
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
                job_id=params['job_id'],
                flow_id=params['flow_id'],
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
                job_id=params['job_id'],
                flow_id=params['flow_id'],
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
                job_id=params['job_id'],
                flow_id=params['flow_id'],
            )
            return False
    return True
