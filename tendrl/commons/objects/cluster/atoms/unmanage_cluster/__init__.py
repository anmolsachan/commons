import etcd
import json
import time
import uuid

from tendrl.commons import objects

from tendrl.commons.event import Event
from tendrl.commons.flows import utils as flow_utils
from tendrl.commons.message import ExceptionMessage
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import log_utils as logger


class UnmanageCluster(objects.BaseAtom):
    def __init__(self, *args, **kwargs):
        super(UnmanageCluster, self).__init__(*args, **kwargs)

    def run(self):
        try:
            integration_id = self.parameters['TendrlContext.integration_id']
            _cluster = NS.tendrl.objects.Cluster(
                integration_id=integration_id
            ).load()

            if 'Node[]' not in self.parameters:
                try:
                    integration_id_index_key = \
                        "indexes/tags/tendrl/integration/%s" % integration_id
                    _node_ids = etcd_utils.read(
                        integration_id_index_key).value
                    self.parameters["Node[]"] = json.loads(_node_ids)

                except etcd.EtcdKeyNotFound:
                    _cluster = NS.tendrl.objects.Cluster(
                        integration_id=NS.tendrl_context.integration_id).load()
                    _cluster.status = ""
                    _cluster.current_job['status'] = 'failed'
                    _cluster.save()
                    raise AtomExecutionFailedError(
                        "Could not execute UnmanageCluster atom - "
                        "Could not load Nodelist from etcd")

            node_list = self.parameters['Node[]']
            if len(node_list) > 1:
                # This is the master node for this flow
                # Lock nodes
                flow_utils.acquire_node_lock(self.parameters)
                NS.tendrl_context = NS.tendrl_context.load()
                # Creating child jobs for nodes to unamange themselves
                for node in node_list:
                    if NS.node_context.node_id != node:
                        new_params = self.parameters.copy()
                        new_params['Node[]'] = [node]
                        # create same flow for each node in node list except
                        #  $this
                        payload = {"tags": ["tendrl/node_%s" % node],
                                   "run": "tendrl.flows.UnmanageCluster",
                                   "status": "new",
                                   "parameters": new_params,
                                   "parent": self.parameters['job_id'],
                                   "type": "node"
                                   }
                        _job_id = str(uuid.uuid4())
                        NS.tendrl.objects.Job(
                            job_id=_job_id,
                            status="new",
                            payload=payload
                        ).save()
                        logger.log(
                            "info",
                            NS.publisher_id,
                            {"message": "UnmanageCluster %s (jobID: %s) :"
                                        "removing host %s" %
                                        (_cluster.short_name, _job_id, node)},
                            job_id=self.parameters['job_id'],
                            flow_id=self.parameters['flow_id']
                        )
                logger.log(
                    "info",
                    NS.publisher_id,
                    {"message": "UnmanageCluster %s waiting for hosts %s "
                                "to be unmanaged"
                                % (_cluster.short_name, node_list)},
                    job_id=self.parameters['job_id'],
                    flow_id=self.parameters['flow_id']
                )
                loop_count = 0
                # Wait for (no of nodes) * 6 minutes for unmanage to complete
                wait_count = (len(node_list) - 1) * 36
                while True:
                    parent_job = NS.tendrl.objects.Job(
                        job_id=self.parameters['job_id']
                    ).load()
                    if loop_count >= wait_count:
                        logger.log(
                            "info",
                            NS.publisher_id,
                            {"message": "Unmanage jobs on cluster(%s) not yet "
                                        "complete on all nodes(%s). "
                                        "Timing out." %
                                        (_cluster.short_name, str(node_list))
                             },
                            job_id=self.parameters['job_id'],
                            flow_id=self.parameters['flow_id']
                        )
                        return False
                    time.sleep(10)
                    finished = True
                    for child_job_id in parent_job.children:
                        child_job = NS.tendrl.objects.Job(
                            job_id=child_job_id
                        ).load()
                        if child_job.status != "finished":
                            finished = False
                            break
                    if finished:
                        break
                    else:
                        loop_count += 1
                        continue

        except Exception as ex:
            # For traceback
            Event(
                ExceptionMessage(
                    priority="error",
                    publisher=NS.publisher_id,
                    payload={
                        "message": ex.message,
                        "exception": ex
                    }
                )
            )
            # raising exception to mark job as failed
            raise ex
        finally:
            # release lock
            flow_utils.release_node_lock(self.parameters)
        return True
