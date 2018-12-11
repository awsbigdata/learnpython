from boto.connection import AWSAuthConnection

class ESConnection(AWSAuthConnection):

    def __init__(self, region, **kwargs):
        super(ESConnection, self).__init__(**kwargs)
        self._set_auth_region_name(region)
        self._set_auth_service_name("es")

    def _required_auth_capability(self):
        return ['hmac-v4']

if __name__ == "__main__":

    client = ESConnection(
            region='us-east-1',
            host='search-testes-gfdjevcuot7pdgg3ntzhpt77u4.us-east-1.es.amazonaws.com', is_secure=False)

    print 'Registering Snapshot Repository'
    resp = client.make_request(method='PUT',
            path='/_snapshot/weblogs-index-backups',
            data='{"type": "s3","settings": { "bucket": "athenaiad","region": "us-east-1","role_arn": "arn:aws:iam::898623153764:role/es-snapshots-role"}}',
            headers={'Content-Type': 'application/json'})
    body = resp.read()
    print body