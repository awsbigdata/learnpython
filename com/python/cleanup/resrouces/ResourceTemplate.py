import boto3
import botocore


class Resource:
    """
    Base class for AWS services.
    """

    TEMPLATE_URL = None

    def __init__(self, region, session):
        self.session = session
        self.client = session.client(
            self._set_service(),
            region_name=region,
            config=botocore.client.Config(max_pool_connections=200)
            )
        self.servicename = self.client._endpoint._endpoint_prefix
        return

    def _set_service(self, service):
        """
        convenience function to set service name
        """
        return service

    def get(self, tags=[]):
        """
        Returns a list of resource objects for the service/region.

        Must be in the format:
        [
            {
                "id": "my-id",
                "name": "my-optional-resource-name",
                "created": datetime,
                "tags": [
                    {
                        "Key": "my-tag-key",
                        "Name": "my-tag-value"
                    }
                ]
            },
            ...
        ]

        If the service does not implement a way to get created time,
        return None for "created".
        If it does not implement tags, return an empty list for "tags",
        can optionally include a "name" key for displaying a friendly name
        """
        return []

    def delete(self, resourceid):
        """
        Deletes the specified resource
        """
        return

    @classmethod
    def is_global_service(cls):
        """
        function to define if running a Global service, like S3, IAM, CF, etc.
        IF set to True, it will run the listing/deleting calls from the session region
        """
        return False
