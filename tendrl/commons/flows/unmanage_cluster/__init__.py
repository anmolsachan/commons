import etcd
import time
import uuid

from tendrl.commons import flows

from tendrl.commons.flows.exceptions import FlowExecutionFailedError
from tendrl.commons.objects import AtomExecutionFailedError
from tendrl.commons.utils import etcd_utils
from tendrl.commons.utils import log_utils as logger


class UnmanageCluster(flows.BaseFlow):
    def __init__(self, *args, **kwargs):
        super(UnmanageCluster, self).__init__(*args, **kwargs)

    def run(self):
        integration_id = self.parameters['TendrlContext.integration_id']
        _cluster = NS.tendrl.objects.Cluster(
            integration_id=integration_id
        ).load()
        if _cluster.is_managed == "no":
            if _cluster.current_job['job_name'] == self.__class__.__name__ \
                    and _cluster.current_job['status'] == 'finished':
                raise FlowExecutionFailedError(
                    "Cluster is already in un-managed state"
                )
        if (_cluster.status is not None and
                _cluster.status != "" and
                _cluster.current_job['status'] == 'in_progress' and
                _cluster.status in
                ["importing", "unmanaging", "expanding"]):
            # Checking if the cluster is being unmanaged by the parent job
            _job = NS.tendrl.objects.Job(job_id=self.job_id).load()
            if 'parent' in _job.payload:
                if _job.payload['parent'] != _cluster.locked_by['job_id']:
                    raise FlowExecutionFailedError(
                        "Another job in progress for cluster, "
                        "please wait till the job finishes (job_id: %s) "
                        "(integration_id: %s) " % (
                            _cluster.current_job['job_id'],
                            _cluster.integration_id
                        )
                    )
            else:
                raise FlowExecutionFailedError(
                    "Another job in progress for cluster, please wait till "
                    "the job finishes (job_id: %s) (integration_id: %s) " % (
                        _cluster.current_job['job_id'],
                        _cluster.integration_id
                    )
                )

        try:
            if 'Node[]' not in self.parameters:
                _lock_details = {
                    'node_id': NS.node_context.node_id,
                    'fqdn': NS.node_context.fqdn,
                    'tags': NS.node_context.tags,
                    'type': NS.type,
                    'job_name': self.__class__.__name__,
                    'job_id': self.job_id
                }
                _cluster.is_managed = "no"
                _cluster.locked_by = _lock_details
                _cluster.status = "unmanaging"
                _cluster.current_job = {
                    'job_id': self.job_id,
                    'job_name': self.__class__.__name__,
                    'status': "in_progress"
                }
                _cluster.save()

            super(UnmanageCluster, self).run()

            # Below code to be executed on the parent node.

            _job = NS.tendrl.objects.Job(job_id=self.job_id).load()
            if 'parent' not in _job.payload:
                # Creating job to delete monitoring details.
                _job_id = str(uuid.uuid4())
                payload = {
                    "tags": ["tendrl/integration/monitoring"],
                    "run": "monitoring.flows.DeleteMonitoringData",
                    "status": "new",
                    "parameters": self.parameters,
                    "parent": self.parameters['job_id'],
                    "type": "monitoring"
                }
                NS.tendrl.objects.Job(
                    job_id=_job_id,
                    status="new",
                    payload=payload
                ).save()

                # Wait for 2 mins for the job to complete
                loop_count = 0
                wait_count = 24
                while True:
                    if loop_count >= wait_count:
                        logger.log(
                            "error",
                            NS.publisher_id,
                            {
                                "message": "Clearing monitoring data for "
                                           "cluster (%s) not yet complete. "
                                           "Timing out." %
                                           NS.tendrl.objects.Cluster(
                                               integration_id=integration_id
                                           ).load().short_name
                            },
                            job_id=self.parameters['job_id'],
                            flow_id=self.parameters['flow_id'],
                        )
                        return False
                    time.sleep(5)
                    finished = True
                    job = NS.tendrl.objects.Job(job_id=_job_id).load()
                    if job.status != "finished":
                        finished = False
                    if finished:
                        break
                    else:
                        loop_count += 1
                        continue

                # Deleting cluster details
                etcd_keys_to_delete = []
                etcd_keys_to_delete.append(
                    "/clusters/%s/nodes" % integration_id
                )
                etcd_keys_to_delete.append(
                    "/clusters/%s/Bricks" % integration_id
                )
                etcd_keys_to_delete.append(
                    "/clusters/%s/Volumes" % integration_id
                )
                etcd_keys_to_delete.append(
                    "/clusters/%s/GlobalDetails" % integration_id
                )
                etcd_keys_to_delete.append(
                    "/clusters/%s/TendrlContext" % integration_id
                )
                etcd_keys_to_delete.append(
                    "/clusters/%s/Utilization" % integration_id
                )
                etcd_keys_to_delete.append(
                    "/clusters/%s/raw_map" % integration_id
                )
                etcd_keys_to_delete.append(
                    "/alerting/clusters/%s" % integration_id
                )
                nodes = etcd_utils.read(
                    "/clusters/%s/nodes" % integration_id
                )
                node_ids = []
                for node in nodes.leaves:
                    node_id = node.key.split("/")[-1]
                    node_ids.append(node_id)
                    key = "/alerting/nodes/%s" % node_id
                    etcd_keys_to_delete.append(
                        key
                    )
                    try:
                        # delete node alerts from /alerting/alerts
                        node_alerts = etcd_utils.read(key)
                        for node_alert in node_alerts.leaves:
                            etcd_keys_to_delete.append(
                                "/alerting/alerts/%s" % node_alert.key.split(
                                    "/")[-1]
                            )
                    except etcd.EtcdKeyNotFound:
                        # No node alerts, continue
                        pass

                # Find the alerting/alerts entries to be deleted
                try:
                    cluster_alert_ids = etcd_utils.read(
                        "/alerting/clusters/%s" % integration_id
                    )
                    for entry in cluster_alert_ids.leaves:
                        ca_id = entry.key.split("/")[-1]
                        etcd_keys_to_delete.append(
                            "/alerting/alerts/%s" % ca_id
                        )
                except etcd.EtcdKeyNotFound:
                    # No cluster alerts, continue
                    pass

                # Remove the cluster details
                for key in list(set(etcd_keys_to_delete)):
                    try:
                        etcd_utils.delete(key, recursive=True)
                    except etcd.EtcdKeyNotFound:
                        logger.log(
                            "debug",
                            NS.publisher_id,
                            {
                                "message": "%s key not found for deletion" %
                                           key
                            },
                            job_id=self.parameters['job_id'],
                            flow_id=self.parameters['flow_id'],
                        )
                        continue
                # remove short name
                _cluster = NS.tendrl.objects.Cluster(
                    integration_id=integration_id
                ).load()
                _cluster.short_name = ""
                _cluster.status = ""
                _cluster.is_managed = "no"
                _cluster.locked_by = {}
                _cluster.errors = []
                _cluster.current_job = {
                    'status': "finished",
                    'job_name': self.__class__.__name__,
                    'job_id': self.job_id
                }
                _cluster.save()
        except (FlowExecutionFailedError,
                AtomExecutionFailedError,
                Exception) as ex:
            _cluster = NS.tendrl.objects.Cluster(
                integration_id=integration_id
            ).load()
            _cluster.status = ""
            _cluster.locked_by = {}
            _cluster.current_job = {
                'status': "failed",
                'job_name': self.__class__.__name__,
                'job_id': self.job_id
            }
            _errors = []
            if hasattr(ex, 'message'):
                _errors = [ex.message]
            else:
                _errors = [str(ex)]
            if _errors:
                _cluster.errors = _errors
            _cluster.save()
            raise ex
