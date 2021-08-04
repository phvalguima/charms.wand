import uuid
from wand.apps.relations.relation_manager_base import RelationManagerBase


class BasePrometheusMonitor(RelationManagerBase):

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)

    def scrape_request(self,
                       port,
                       metrics_path,
                       endpoint,
                       ca_cert=None):
        """Request registers the Prometheus scrape job.
        port: to be used as part of the target
        """
        # advertise_addr given that minio endpoint uses advertise_addr
        # to find its hostname
        name = "{}_node".format(self._charm.unit.name.replace("/", "-"))
        data = {
            'job_name': name,
            'job_data': {
                'static_configs': [{
                    'targets': ["{}:{}".format(
                        endpoint or self.advertise_addr, port)]
                }],
                'scheme': 'http',
                'metrics_path': metrics_path
            }
        }
        if ca_cert:
            data['tls_config'] = {'ca_file': '__ca_file__'}
            data['scheme'] = 'https'
            self.request(name, ca_cert=ca_cert, job_data=data)
            return
        self.request(name, job_data=data)

    # Keeping it for compatibility with other charms
    def request(self, job_name, ca_cert=None, job_data=None):
        # Job name as field and value the json describing it
        req_uuid = str(uuid.uuid4())
        job_data["request_id"] = req_uuid
        self.send("request_" + req_uuid, job_data)
