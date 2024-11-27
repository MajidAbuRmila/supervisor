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

import requests

from spaceone.core.error import ERROR_CONFIGURATION
from spaceone.supervisor.connector.container_connector import ContainerConnector

# Configuring a logger for this module
_LOGGER = logging.getLogger(__name__)

# Defining constants
HTTP_REQUEST_TIMEOUT = 30
DEFAULT_BASE_URL = 'https://api.cpln.io'
BASE_URL =  os.getenv('CPLN_ENDPOINT', DEFAULT_BASE_URL) or DEFAULT_BASE_URL

class CplnConnector(ContainerConnector):
    def __init__(self, *args, **kwargs):
        """
        Initializes the CplnConnector and verifies authorization.

        :param token: Bearer token for authentication
        :param org: Organization name
        :param gvc: Global Virtual Cloud name
        :raises ERROR_CONFIGURATION: If authorization or GVC verification fails
        """

        super().__init__(*args, **kwargs)

        # Extracting required parameters from the configuration
        self.token = self.config.get('token', os.getenv('CPLN_TOKEN'))
        self.org = self.config.get('org', os.getenv('CPLN_ORG'))
        self.gvc = self.config.get('gvc', os.getenv('CPLN_GVC'))

        # Logging the configuration for debugging purposes
        _LOGGER.debug(f'[CplnConnector] config: {self.sanitize_config(self.config)}')

        # Verifying authorization by ensuring the org and GVC exist
        self._verify_authorization()

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
        :return: Plugin information extracted from the workload
        :raises ERROR_CONFIGURATION: If the workload creation or retrieval fails
        """
        try:
            # Attempt to retrieve or create the workload
            workload = self._get_or_create_workload(image, labels, ports, name)

            # Return the extracted plugin information from the workload
            return self._extract_plugin_info(workload)
        except Exception as e:
            error_message = f"Failed to run workload '{name}': {str(e)}"
            _LOGGER.error(f"[run] {error_message}")
            raise ERROR_CONFIGURATION(error_message)

    def stop(self, plugin: dict) -> bool:
        """
        Stops a workload.

        :param plugin: Plugin dictionary containing the workload's information
        :return: True if the workload was successfully stopped
        :raises ERROR_CONFIGURATION: If stopping the workload fails
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
            raise ERROR_CONFIGURATION(error_message)

    ### Protected Methods ###

    def _verify_authorization(self):
        """
        Verifies if the organization and GVC exist and are accessible.

        :raises ERROR_CONFIGURATION: If the organization or GVC cannot be verified
        """
        _LOGGER.debug(f'[CplnConnector] inside _verify_authorization')

        try:
            # Verify organization existence via GET request
            self._get_resource(f"/org/{self.org}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                error_message = f"Organization '{self.org}' does not exist."
            else:
                error_message = f"Authorization failed: {str(e)}"
            _LOGGER.error(f"[_verify_authorization] {error_message}")
            raise ERROR_CONFIGURATION(error_message)
        except Exception as e:
            error_message = f"Failed to verify Org: {str(e)}"
            _LOGGER.error(f"[_verify_authorization] {error_message}")
            raise ERROR_CONFIGURATION(error_message)

        try:
            # Verify GVC existence via GET request
            self._get_resource(f"/org/{self.org}/gvc/{self.gvc}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                error_message = f"GVC '{self.gvc}' does not exist."
            else:
                error_message = f"Authorization failed: {str(e)}"
            _LOGGER.error(f"[_verify_authorization] {error_message}")
            raise ERROR_CONFIGURATION(error_message)
        except Exception as e:
            error_message = f"Failed to verify GVC: {str(e)}"
            _LOGGER.error(f"[_verify_authorization] {error_message}")
            raise ERROR_CONFIGURATION(error_message)

    def _fetch_workloads_by_label(self, label: str) -> list:
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
            raise ERROR_CONFIGURATION(error_message)

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
        :raises ERROR_CONFIGURATION: If the workload creation or retrieval fails
        """
        try:
            # Attempt to retrieve the workload by its name
            return self._get_resource(f"/org/{self.org}/gvc/{self.gvc}/workload/{name}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                error_message = f"Failed to retrieve workload '{name}': {str(e)}"
                _LOGGER.error(f"[_get_or_create_workload] {error_message}")
                raise ERROR_CONFIGURATION(error_message)

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
        :raises ERROR_CONFIGURATION: If the workload creation fails
        """
        # Define the workload structure to be sent to the API
        new_workload = {
            "kind": "workload",
            "name": name,
            "tags": self._map_labels_to_tags(labels),
            "spec": {
                "containers": [
                    {
                        "name": self._extract_container_name(image),
                        "image": image,
                        "ports": [{"number": ports["TargetPort"], "protocol": "http"}],
                    }
                ]
            },
        }
        try:
            # Send a POST request to create the workload
            self._post_resource(f"/org/{self.org}/gvc/{self.gvc}/workload", new_workload)

            # Retrieve the newly created workload for confirmation
            return self._get_resource(f"/org/{self.org}/gvc/{self.gvc}/workload/{name}")
        except Exception as e:
            error_message = f"Failed to create workload '{name}': {str(e)}"
            _LOGGER.error(f"[_create_workload] {error_message}")
            raise ERROR_CONFIGURATION(error_message)

    def _extract_plugin_info(self, workload: dict) -> dict:
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
            "status": self._determine_workload_status(workload),
        }

    def _determine_workload_status(self, workload: dict) -> str:
        """
        Determines the status of a workload based on deployments.

        :param workload: The workload dictionary
        :return: Status string ("ACTIVE" or "NOT ACTIVE")
        """
        deployment_link = ""

        # Extract the deployment link from workload links
        for link in workload["links"]:
            if link["rel"] == "deployment":
                deployment_link = link["href"]
                break

        # If no deployment link exists, the workload is not active
        if not deployment_link:
            return "NOT ACTIVE"

        # Retrieve deployments associated with the workload
        try:
            deployments = self._get_resource(deployment_link).get("items")
        except requests.exceptions.RequestException as e:
            error_message = f"Failed to fetch workloads: {e}"
            _LOGGER.error(f"[_determine_workload_status] {error_message}")
            raise ERROR_CONFIGURATION(error_message)

        # If no deployments exist, the workload is not active
        if len(deployments) < 1:
            return "NOT ACTIVE"

        # Variables to track the latest ready deployments and total locations
        latest_ready = 0
        total_locations = len(deployments)

        # Check the status of each deployment
        for deployment in deployments:
            deployment_status = deployment.get("status", {})
            expected_deployment_version = deployment_status.get("expectedDeploymentVersion")

            # Increment the ready count if the deployment is ready and not failed
            if deployment_status.get("ready", False) and not deployment_status.get("internal", {}).get("syncFailed"):
                latest_version = next(
                    (v for v in deployment_status.get("versions", []) if v.get("workload", "") == expected_deployment_version), None
                )
                if latest_version and latest_version.get("ready"):
                    latest_ready += 1

        # Adjust total locations for workloads with local options (e.g., autoscaling or suspension)
        for local_option in workload.get("spec", {}).get("localOptions", []):
            if (
                    local_option.get("suspend") or
                    (
                            local_option.get("autoscaling", {}).get("minScale") == 0 and
                            local_option.get("autoscaling", {}).get("maxScale") == 0
                    )
            ):
                total_locations -= 1

        # Cron-type workloads are always active
        if workload["spec"]["type"] == "cron":
            return "ACTIVE"

        # Determine the final status based on readiness
        return "ACTIVE" if latest_ready >= total_locations else "NOT ACTIVE"

    ## API Request Helpers

    def _get_resource(self, path: str) -> dict:
        """
        Sends a GET request to the specified API path.

        :param path: Resource path
        :return: JSON response as a dictionary
        :raises ERROR_CONFIGURATION: If the GET request fails
        """
        # Construct the full URL
        url = f"{BASE_URL}{path}"

        # Set up headers for authentication and content type
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

        # Send the GET request
        response = requests.get(url, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)

        # Raise an exception if the response indicates an error
        response.raise_for_status()

        # Return the JSON payload of the response
        return response.json()

    def _post_resource(self, path: str, body: dict):
        """
        Sends a POST request to the specified resource path.

        :param path: Resource path
        :param body: JSON body of the request
        :raises ERROR_CONFIGURATION: If the POST request fails
        """
        # Construct the full URL
        url = f"{BASE_URL}{path}"

        # Set up headers for authentication and content type
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

        # Send the POST request with the provided body
        response = requests.post(url, headers=headers, json=body, timeout=HTTP_REQUEST_TIMEOUT)

        # Raise an exception if the response indicates an error
        response.raise_for_status()

    def _delete_resource(self, path: str):
        """
        Sends a DELETE request to the specified resource path.

        :param path: Resource path
        :raises ERROR_CONFIGURATION: If the DELETE request fails
        """
        # Construct the full URL
        url = f"{BASE_URL}{path}"

        # Set up headers for authentication and content type
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

        # Send the DELETE request
        response = requests.delete(url, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)

        # Raise an exception if the response indicates an error
        response.raise_for_status()

    ### Static Methods ###

    @staticmethod
    def _extract_container_name(image: str) -> str:
        """
        Safely extracts the container name from a Docker image string.

        :param image: Docker image string
        :return: Extracted container name
        :raises ValueError: If the image string format is invalid
        """
        # Validate that the input is a non-empty string
        if not image or not isinstance(image, str):
            raise ValueError("Image must be a non-empty string.")

        # Split the image string by '/' and extract the last segment
        segments = image.split("/")
        if not segments:
            raise ValueError(f"Invalid image format: '{image}'")

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
            raise ERROR_CONFIGURATION(error_message)

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
            if k == "spaceone.supervisor.name":
                tags["supervisor_name"] = v
            elif k == "spaceone.supervisor.domain_id":
                tags["domain_id"] = v
            elif k == "spaceone.supervisor.plugin_id":
                tags["plugin_id"] = v
            elif k == "spaceone.supervisor.plugin.version":
                tags["version"] = v
            elif k == "spaceone.supervisor.plugin.resource_type":
                tags["resource_type"] = v

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
        if not labels:
            return False

        # Iterate through each label in the labels list
        for label in labels:
            # Split the label string into a key and value based on the "=" separator
            k, v = label.split("=")

            # Check if the key exists in the tags dictionary and if its value matches the expected value
            if k not in tags or tags[k] != v:
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
