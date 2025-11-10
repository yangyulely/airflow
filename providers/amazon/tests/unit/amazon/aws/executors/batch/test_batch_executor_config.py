# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
import os
from unittest import mock

import pytest

from airflow.providers.amazon.aws.executors.batch.batch_executor_config import (
    _fetch_config_values,
    _fetch_templated_kwargs,
    build_submit_kwargs,
)
from airflow.providers.amazon.aws.executors.batch.utils import (
    CONFIG_GROUP_NAME,
    AllBatchConfigKeys,
)

from tests_common.test_utils.config import conf_vars


class TestBatchExecutorConfig:
    """Tests for the batch executor config functions."""

    def setup_method(self):
        """Clear environment variables before each test."""
        self._unset_conf()

    def teardown_method(self):
        """Clear environment variables after each test."""
        self._unset_conf()

    @staticmethod
    def _unset_conf():
        """Remove all AWS batch executor config environment variables."""
        for env in os.environ:
            if env.startswith(f"AIRFLOW__{CONFIG_GROUP_NAME.upper()}__"):
                os.environ.pop(env)

    def test_fetch_config_values_empty(self):
        """Test _fetch_config_values with no config set."""
        result = _fetch_config_values()
        assert result == {}

    def test_fetch_config_values_with_config(self):
        """Test _fetch_config_values with config set."""
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "test-job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
        }
        with conf_vars(overrides):
            result = _fetch_config_values()
            expected = {
                "job_name": "test-job-name",
                "job_queue": "test-job-queue",
                "job_definition": "test-job-definition",
            }
            assert result == expected

    def test_fetch_templated_kwargs_empty(self):
        """Test _fetch_templated_kwargs with no config set."""
        result = _fetch_templated_kwargs()
        assert result == {}

    def test_fetch_templated_kwargs_with_config(self):
        """Test _fetch_templated_kwargs with config set."""
        submit_job_kwargs = {
            "shareIdentifier": "test-share-id",
            "tags": [{"key": "test", "value": "value"}],
        }
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(submit_job_kwargs),
        }
        with conf_vars(overrides):
            result = _fetch_templated_kwargs()
            assert result == submit_job_kwargs

    def test_fetch_templated_kwargs_invalid_json(self):
        """Test _fetch_templated_kwargs with invalid JSON."""
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): "invalid-json",
        }
        with conf_vars(overrides):
            with pytest.raises(json.JSONDecodeError):
                _fetch_templated_kwargs()

    def test_build_submit_kwargs_defaults(self):
        """Test build_submit_kwargs with default configuration."""
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "test-job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
        }
        with conf_vars(overrides):
            result = build_submit_kwargs()
            expected = {
                "jobName": "test-job-name",
                "jobQueue": "test-job-queue",
                "jobDefinition": "test-job-definition",
                "containerOverrides": {
                    "command": [],
                },
            }
            assert result == expected

    def test_build_submit_kwargs_with_templated_kwargs(self):
        """Test build_submit_kwargs with templated kwargs."""
        submit_job_kwargs = {
            "shareIdentifier": "test-share-id",
            "tags": [{"key": "test", "value": "value"}],
            "containerOverrides": {
                "memory": 512,
                "vcpus": 2,
            },
        }
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "test-job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(submit_job_kwargs),
        }
        with conf_vars(overrides):
            result = build_submit_kwargs()
            expected = {
                "jobName": "test-job-name",
                "jobQueue": "test-job-queue",
                "jobDefinition": "test-job-definition",
                "shareIdentifier": "test-share-id",
                "tags": [{"key": "test", "value": "value"}],
                "containerOverrides": {
                    "command": [],
                    "memory": 512,
                    "vcpus": 2,
                },
            }
            assert result == expected

    def test_build_submit_kwargs_with_nested_templated_kwargs(self):
        """Test build_submit_kwargs with nested templated kwargs."""
        submit_job_kwargs = {
            "retryStrategy": {
                "attempts": 3,
                "evaluateOnExit": [
                    {
                        "onStatusReason": "Host EC2*",
                        "action": "RETRY",
                    }
                ],
            },
            "containerOverrides": {
                "environment": [
                    {"name": "TEST_ENV", "value": "test_value"},
                ],
            },
        }
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "test-job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(submit_job_kwargs),
        }
        with conf_vars(overrides):
            result = build_submit_kwargs()
            expected = {
                "jobName": "test-job-name",
                "jobQueue": "test-job-queue",
                "jobDefinition": "test-job-definition",
                "retryStrategy": {
                    "attempts": 3,
                    "evaluateOnExit": [
                        {
                            "onStatusReason": "Host EC2*",
                            "action": "RETRY",
                        }
                    ],
                },
                "containerOverrides": {
                    "command": [],
                    "environment": [
                        {"name": "TEST_ENV", "value": "test_value"},
                    ],
                },
            }
            assert result == expected

    def test_build_submit_kwargs_missing_container_overrides(self):
        """Test build_submit_kwargs creates containerOverrides if missing."""
        submit_job_kwargs = {
            "shareIdentifier": "test-share-id",
        }
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "test-job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(submit_job_kwargs),
        }
        with conf_vars(overrides):
            result = build_submit_kwargs()
            expected = {
                "jobName": "test-job-name",
                "jobQueue": "test-job-queue",
                "jobDefinition": "test-job-definition",
                "shareIdentifier": "test-share-id",
                "containerOverrides": {
                    "command": [],
                },
            }
            assert result == expected

    def test_build_submit_kwargs_multi_node_jobs_not_supported(self):
        """Test build_submit_kwargs raises error for multi-node jobs."""
        submit_job_kwargs = {
            "nodeOverrides": {
                "nodePropertyOverrides": [
                    {
                        "targetNodes": "0:1",
                        "containerOverrides": {"vcpus": 2},
                    }
                ]
            }
        }
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "test-job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(submit_job_kwargs),
        }
        with conf_vars(overrides):
            with pytest.raises(KeyError, match="Multi-node jobs are not currently supported."):
                build_submit_kwargs()

    def test_build_submit_kwargs_eks_jobs_not_supported(self):
        """Test build_submit_kwargs raises error for EKS jobs."""
        submit_job_kwargs = {
            "eksPropertiesOverride": {
                "podProperties": {
                    "hostNetwork": True,
                }
            }
        }
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "test-job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(submit_job_kwargs),
        }
        with conf_vars(overrides):
            with pytest.raises(KeyError, match="Eks jobs are not currently supported."):
                build_submit_kwargs()

    def test_build_submit_kwargs_non_serializable_raises_error(self):
        """Test build_submit_kwargs raises error when config is not JSON serializable."""
        # Mock the camelize_dict_keys function to return a non-serializable object
        with mock.patch(
            "airflow.providers.amazon.aws.executors.batch.batch_executor_config.camelize_dict_keys"
        ) as mock_camelize:
            mock_camelize.return_value = {"test": mock.Mock()}
            overrides = {
                (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "test-job-name",
                (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
                (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
            }
            with conf_vars(overrides):
                with pytest.raises(TypeError, match="Object of type Mock is not JSON serializable"):
                    build_submit_kwargs()

    def test_build_submit_kwargs_templated_kwargs_override_config_values(self):
        """Test that templated kwargs override config values."""
        submit_job_kwargs = {
            "jobName": "templated-job-name",  # This should override config
            "shareIdentifier": "test-share-id",
        }
        overrides = {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "config-job-name",  # This will be overridden
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "test-job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "test-job-definition",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(submit_job_kwargs),
        }
        with conf_vars(overrides):
            result = build_submit_kwargs()
            # The jobName from templated kwargs should override the one from config
            assert result["jobName"] == "templated-job-name"
            assert result["shareIdentifier"] == "test-share-id"
