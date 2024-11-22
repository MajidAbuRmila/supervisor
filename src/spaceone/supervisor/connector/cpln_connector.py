#
#   Copyright 2020 The SpaceONE Authors.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

__all__ = ["CplnConnector"]

import logging
import os
import time
import requests

from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.supervisor.connector.container_connector import ContainerConnector

# Configuring a logger for this module
_LOGGER = logging.getLogger(__name__)

# Defining constants
BASE_DELAY = 1
MAX_RETRIES = 5
HTTP_REQUEST_TIMEOUT = 30
DEFAULT_BASE_URL = 'https://api.cpln.io'
BASE_URL =  os.getenv('CPLN_ENDPOINT', DEFAULT_BASE_URL) or DEFAULT_BASE_URL

class CplnConnector(ContainerConnector):
    def __init__(self, *args, **kwargs):
        """
        Initializes the CplnConnector and verifies authorization.
        """

        super().__init__(*args, **kwargs)

        # Extracting required parameters from the configuration
        self.token = self.config.get('token', os.getenv('CPLN_TOKEN'))
        self.org = self.config.get('org', os.getenv('CPLN_ORG'))
        self.gvc = self.config.get('gvc', os.getenv('CPLN_GVC'))
        self.replica = self.config.get('replica', {})

        # Logging the configuration for debugging purposes
        _LOGGER.debug(f'[CplnConnector] config: {self.sanitize_config(self.config)}')

    ### Public Methods ###

    def search(self, filters: dict) -> dict:
        """
        Searches for workloads based on the given filters.

        :param filters: A dictionary containing search filters (e.g., "label")
        :return: A dictionary with "results" (list of plugins) and "total_count" (int)
        """
        # Logging the filters for debugging
        _LOGGER.debug(f'[search] filters: {filters}')

        # Initialize an empty list and extract label filter
        plugins_info = []
        label_filter = filters.get("label")

        # Fetch workloads based on label if specified
        workloads = self._fetch_workloads_by_label(label_filter) if label_filter else []

        # Populate plugin info from each workload
        for workload in workloads:
            plugin = self._extract_plugin_info(workload)
            plugins_info.append(plugin)

        return {"results": plugins_info, "total_count": len(workloads)}

    def run(self, image, labels, ports, name, registry_config) -> dict:
        """
        Runs a new workload or retrieves an existing one.

        :param image: Image for the workload
        :param labels: Labels to attach to the workload
        :param ports: Port mapping dictionary (e.g., {"TargetPort": 8080})
        :param name: Name of the workload
        :param registry_config: Additional configuration
        :return: Plugin information extracted from the workload
        :raises Exception: If the workload creation or retrieval fails
        """
        # Convert to a valid name
        workload_name = name[:49]

        try:
            # Attempt to retrieve or create the workload
            workload = self._get_or_create_workload(image, labels, ports, workload_name)

            # Return the extracted plugin information from the workload
            return self._extract_plugin_info(workload)
        except Exception as e:
            error_message = f"Failed to run workload '{workload_name}': {str(e)}"
            _LOGGER.error(f"[run] {error_message}")
            raise e

    def stop(self, plugin: dict) -> bool:
        """
        Stops a workload.

        :param plugin: Plugin dictionary containing the workload's information
        :return: True if the workload was successfully stopped
        :raises Exception: If stopping the workload fails
        """
        # Extract the plugin name from the plugin dictionary
        plugin_name = plugin.get("name", "Unknown Plugin Name")

        try:
            # Send a DELETE request to stop the workload
            self._delete_resource(f"/org/{self.org}/gvc/{self.gvc}/workload/{plugin_name}")
            return True
        except Exception as e:
            error_message = f"Failed to stop workload '{plugin_name}': {str(e)}"
            _LOGGER.error(f"[stop] {error_message}")
            raise e

    ### Protected Methods ###

    def _fetch_workloads_by_label(self, label: [list, str]) -> list:
        """
        Fetches workloads filtered by a specific label.

        :param label: Label to filter workloads
        :return: List of workloads that match the label
        """
        # Initialize an empty list to store filtered workloads
        filtered_workloads = []

        # Fetch all workloads for the organization and GVC
        try:
            workloads = self._get_resource(f"/org/{self.org}/gvc/{self.gvc}/workload")
        except requests.exceptions.RequestException as e:
            error_message = f"Failed to fetch workloads: {e}"
            _LOGGER.error(f"[_fetch_workloads_by_label] {error_message}")
            raise e

        # Ensure labels is a list, even if a single string is provided
        labels = [label] if isinstance(label, str) else label

        # Filter workloads by checking if the label exists in the workload's tags
        for workload in workloads.get("items", []):
            if self._label_exists_in_tags(labels, workload.get("tags", {})):
                filtered_workloads.append(workload)

        return filtered_workloads

    def _get_or_create_workload(self, image: str, labels: dict, ports: dict, name: str) -> dict:
        """
        Retrieves an existing workload or creates a new one.

        :param image: Image for the workload
        :param labels: Labels to attach to the workload
        :param ports: Port mapping dictionary
        :param name: Name of the workload
        :return: The workload dictionary
        :raises HTTPError: If the workload creation or retrieval fails
        """
        try:
            # Attempt to retrieve the workload by its name
            return self._get_resource(f"/org/{self.org}/gvc/{self.gvc}/workload/{name}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                error_message = f"Failed to retrieve workload '{name}': {str(e.response.text)}"
                _LOGGER.error(f"[_get_or_create_workload] {error_message}")
                raise e

        # If workload doesn't exist, create a new one
        return self._create_workload(image, labels, ports, name)

    def _create_workload(self, image: str, labels: dict, ports: dict, name: str) -> dict:
        """
        Creates a new workload.

        :param image: Image for the workload
        :param labels: Labels to attach to the workload
        :param ports: Port mapping dictionary
        :param name: Name of the workload
        :return: The newly created workload
        :raises Exception: If the workload creation fails
        """
        # Get tags
        tags = self._map_labels_to_tags(labels)

        # Define number of replicas
        replica_count = 1

        # Get number of replicas
        if "spaceone.supervisor.plugin.resource_type" in tags and isinstance(self.replica, dict):
            # Extract resource type from tags
            resource_type = tags["spaceone.supervisor.plugin.resource_type"]

            # Attempt to extract plugin id
            plugin_id = tags.get("spaceone.supervisor.plugin.plugin_id", None)

            # Define new variable for a possible key in configured replica
            resource_type_with_plugin_id = f"{resource_type}?{plugin_id}"

            # Declare replica value
            replica_value = None

            # Extract replica value as conditioned below
            if resource_type_with_plugin_id in self.replica:
                replica_value = self.replica[resource_type_with_plugin_id]
            elif resource_type in self.replica:
                replica_value = self.replica[resource_type]

            # Attempt to get replica count only if replica value is defined
            if replica_value:
                try:
                    # Try converting the configured replica to an int
                    replica_count = int(replica_value)
                except ValueError:
                    _LOGGER.debug(f"[_create_workload] replica is not an integer. Replica: {replica_value}, Resource Type: {resource_type}.")


        # Define the workload structure to be sent to the API
        new_workload = {
            "kind": "workload",
            "name": name[:49],
            "tags": tags,
            "spec": {
                "containers": [
                    {
                        "name": self._extract_container_name(image),
                        "image": image,
                        "cpu": "100m",
                        "memory": "256Mi",
                        "minCpu": "100m",
                        "minMemory": "256Mi",
                        "ports": [
                            {
                                "number": ports["TargetPort"],
                                "protocol": self.config.get("protocol", "grpc")
                            }
                        ],
                    }
                ],
                "defaultOptions": {
                    "capacityAI": False,
                    "debug": False,
                    "suspend": False,
                    "timeoutSeconds": 15,
                    "autoscaling": {
                        "metric": "disabled",
                        "minScale": replica_count,
                        "maxScale": replica_count,
                        "maxConcurrency": 1000,
                        "scaleToZeroDelay": 300,
                        "target": 100,
                    },
                },
                "firewallConfig": {
                    "external": {
                        "outboundAllowCIDR": ['0.0.0.0/0'],
                    },
                    "internal": {
                        "inboundAllowType": "same-gvc"
                    }
                }
            },
        }
        try:
            # Send a POST request to create the workload
            self._post_resource(f"/org/{self.org}/gvc/{self.gvc}/workload", new_workload)

            # Retrieve the newly created workload for confirmation
            return self._get_resource(f"/org/{self.org}/gvc/{self.gvc}/workload/{name}")
        except requests.exceptions.HTTPError as e:
            error_message = f"Failed to create workload '{name}': {str(e.response.text)}"
            _LOGGER.error(f"[_create_workload] {error_message}")
            raise e
        except Exception as e:
            error_message = f"Failed to create workload '{name}': {str(e)}"
            _LOGGER.error(f"[_create_workload] {error_message}")
            raise e

    ## API Request Helpers

    def _get_resource(self, path: str) -> dict:
        """
        Sends a GET request to the specified API path.

        :param path: Resource path
        :return: JSON response as a dictionary
        """
        _LOGGER.debug(f"[_get_resource] making GET request to {path}")

        # Make the GET request to the given path using the centralized retry logic
        response = self._make_request_with_retry("get", path)

        # Parse the response as JSON and return it to the caller
        return response.json()

    def _post_resource(self, path: str, body: dict):
        """
        Sends a POST request to the specified resource path.

        :param path: Resource path
        :param body: JSON body of the request
        """
        _LOGGER.debug(f"[_post_resource] making POST request to {path}")

        # Make the POST request to the given path using the centralized retry logic
        self._make_request_with_retry("post", path, json=body)

    def _delete_resource(self, path: str):
        """
        Sends a DELETE request to the specified resource path.

        :param path: Resource path
        """
        _LOGGER.debug(f"[_delete_resource] making DELETE request to {path}")

        # Make the DELETE request to the given path using the centralized retry logic
        self._make_request_with_retry("delete", path)

    def _make_request_with_retry(self, method, path, **kwargs):
        """
        Centralized retry logic for HTTP requests.

        :param method: The HTTP method to use (e.g., 'get', 'post', 'delete').
        :param path: The API path to make the request to.
        :param kwargs: Additional keyword arguments to pass to the `requests.request` method.
        :return: The `requests.Response` object if the request succeeds.
        :raises Exception: If all retry attempts fail or if a non-429 error occurs.
        :raises HTTPError: If status code is >= 400.
        """
        # Initialize the retry count to track the number of attempts
        retry_count = 0

        # Construct the full URL by combining the base URL and the given path
        url = f"{BASE_URL}{path}"

        # Create headers for the request, including the authorization token and content type
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

        # Ensure that the headers are included in the keyword arguments
        kwargs["headers"] = headers

        # Start the retry loop; it will run until the retry limit is reached
        while retry_count <= MAX_RETRIES:
            # Log retries, but not the first attempt
            if retry_count > 0:
                _LOGGER.debug(f"[_make_request_with_retry] {method.upper()} request to {url}, Retry {retry_count}")

            # Perform the HTTP request using the `requests.request` method
            response = requests.request(method, url, timeout=HTTP_REQUEST_TIMEOUT, **kwargs)

            # If the response status code is not 429, handle it as a normal response
            if response.status_code != 429:
                # If the response is not successful (status codes >= 400), raise an exception
                response.raise_for_status()

                # Return the response object for successful requests
                return response

            # If the response status code is 429, log a warning and prepare to retry
            _LOGGER.warning(f"[_make_request_with_retry] Received 429 for {method.upper()} {url} Error: {response.text}, retrying...")

            # Increment the retry count for the next attempt
            retry_count += 1

            # Calculate the delay using exponential backoff: BASE_DELAY * 2^retry_count
            delay = BASE_DELAY * (2 ** retry_count)

            # Log the delay to keep track of wait times between retries
            _LOGGER.debug(f"Retry {retry_count}: Waiting for {delay} seconds before retrying request...")

            # Pause execution for the calculated delay time before retrying
            time.sleep(delay)

        # If all retries are exhausted, raise an exception indicating the failure
        raise Exception(f"Failed after {MAX_RETRIES} retries for {method.upper()} {url}")

    ### Static Methods ###

    @staticmethod
    def _extract_plugin_info(workload: dict) -> dict:
        """
        Extracts plugin information from a workload.

        :param workload: The workload dictionary
        :return: Plugin information as a dictionary
        """
        tags = workload.get("tags", {})

        # Construct the plugin information dictionary
        return {
            "plugin_id": tags.get("spaceone.supervisor.plugin_id", "Unknown"),
            "image": tags.get("spaceone.supervisor.plugin.image", "Unknown"),
            "version": tags.get("spaceone.supervisor.plugin.version", "Unknown"),
            "endpoint": tags.get("spaceone.supervisor.plugin.endpoint", "Unknown"),
            "labels": tags,
            "name": workload["name"],
            "status": "ACTIVE"
        }

    @staticmethod
    def _extract_container_name(image: str) -> str:
        """
        Safely extracts the container name from a Docker image string.

        :param image: Docker image string
        :return: Extracted container name
        :raises ERROR_CONFIGURATION: If the image string format is invalid
        """
        # Validate that the input is a non-empty string
        if not image or not isinstance(image, str):
            error_message = "Image must be a non-empty string."
            _LOGGER.error(f"[_extract_container_name] {error_message}")
            raise ERROR_CONFIGURATION(key=error_message)

        # Split the image string by '/' and extract the last segment
        segments = image.split("/")
        if not segments:
            error_message = "Invalid image format: '{image}'."
            _LOGGER.error(f"[_extract_container_name] {error_message}")
            raise ERROR_CONFIGURATION(key=error_message)

        # Extract the last segment and split it by ':' to remove the tag (if any)
        name_with_tag = segments[-1]
        if ":" in name_with_tag:
            name = name_with_tag.split(":")[0]
        else:
            name = name_with_tag

        # Ensure the extracted name is valid
        if not name:
            error_message = f"Could not determine container name from image: '{image}'"
            _LOGGER.error(f"[_extract_container_name] {error_message}")
            raise ERROR_CONFIGURATION(key=error_message)

        return name

    @staticmethod
    def _map_labels_to_tags(labels: dict) -> dict:
        """
        Maps user-provided labels to API-compatible tags.

        :param labels: Dictionary of labels
        :return: Mapped tags
        """
        # Initialize an empty dictionary for tags
        tags = {}

        # Map specific label keys to tag keys for API compatibility
        for k, v in labels.items():
            tags[k] = v

        return tags

    @staticmethod
    def _label_exists_in_tags(labels: list, tags: dict) -> bool:
        """
        Checks if a list of labels exists in a given tags dictionary.

        :param labels: List of labels (key=value)
        :param tags: Dictionary of tags
        :return: True if all labels exist in tags, otherwise False
        """
        # If the labels list is empty or None, immediately return False as no labels exist to check
        # _LOGGER.debug(f"[_label_exists_in_tags] labels: {labels}")

        if not labels:
            # _LOGGER.debug(f"[_label_exists_in_tags] No labels provided.")
            return False

        # Iterate through each label in the labels list
        for label in labels:
            # Split the label string into a key and value based on the "=" separator
            k, v = label.split("=")

            # Check if the key exists in the tags dictionary and if its value matches the expected value
            if k not in tags or tags[k] != v:
                # _LOGGER.debug(f"[_label_exists_in_tags] label: '{label}' was not found in tags: '{tags}'.")
                return False

        # If all labels exist in tags with matching key-value pairs, return True
        return True

    @staticmethod
    def sanitize_config(config: dict) -> dict:
        """
        Redacts sensitive keys in a configuration dictionary, such as 'token',
        while leaving the rest of the dictionary intact.

        :param config: Dictionary containing configuration data.
        :return: A sanitized copy of the configuration dictionary with sensitive
                 keys redacted.
        """
        # Create a shallow copy of the dictionary to avoid modifying the original
        sanitized = config.copy()

        # Check if the 'token' key exists in the dictionary
        if 'token' in sanitized:
            # Redact the value of the 'token' key to prevent it from being logged
            sanitized['token'] = '***'

        # Return the sanitized copy of the configuration
        return sanitized
