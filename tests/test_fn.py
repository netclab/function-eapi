# Copyright 2025-present Michal Bakalarski and Netclab Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
import unittest

from crossplane.function import logging, resource
from crossplane.function.proto.v1 import run_function_pb2 as fnv1
from google.protobuf import struct_pb2 as structpb
from google.protobuf.json_format import MessageToDict

from function import fn


class TestFunctionRunner(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        logging.configure(level=logging.Level.DISABLED)

    async def test_run_function_generates_request(self) -> None:
        """Generates a valid HTTP Request from an CliConfig."""

        # ----------------------------
        # Inputs
        # ----------------------------

        composite = {
            "apiVersion": "netclab.dev/v1alpha1",
            "kind": "CliConfig",
            "metadata": {"name": "eoscommand-1"},
            "spec": {
                "endpoint": "ceos01.default.svc.cluster.local",
                "cmds": {
                    "ip prefix-list PL-Loopback0": {
                        "seq 10 permit 10.0.0.1/32 eq 32": {}
                    }
                },
            },
        }

        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "eos-creds",
                "namespace": "crossplane-system",
            },
            "type": "Opaque",
            "data": {
                "basicAuth": "YXJpc3RhOmFyaXN0YQ==",
            },
        }

        environment = {
            "restconf": {"scheme": "https", "port": 6020},
            "jsonrpc": {"scheme": "http", "port": 6021},
        }

        context = structpb.Struct(
            fields={
                "apiextensions.crossplane.io/environment": structpb.Value(
                    struct_value=resource.dict_to_struct(environment)
                )
            }
        )

        req = fnv1.RunFunctionRequest(
            input=resource.dict_to_struct({"version": "v1beta2"}),
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(composite))
            ),
            required_resources={
                "eos-creds": fnv1.Resources(
                    items=[fnv1.Resource(resource=resource.dict_to_struct(secret))]
                )
            },
            context=context,
        )

        # ----------------------------
        # Run function
        # ----------------------------

        runner = fn.FunctionRunner()
        resp = await runner.RunFunction(req, None)

        # ----------------------------
        # Resource existence
        # ----------------------------

        request_name = "eoscommand-1-1a2c114ed6c93d688a2f215bef12df003efac252912d4ed6ba"

        self.assertGreater(
            len(resp.desired.resources), 0, "No desired resources generated"
        )

        self.assertIn(
            request_name,
            resp.desired.resources,
            "Expected Request resource not found",
        )

        request_resource = resp.desired.resources[request_name].resource

        result = MessageToDict(request_resource)

        # ----------------------------
        # Top-level assertions
        # ----------------------------

        self.assertEqual(result["kind"], "Request")
        self.assertEqual(result["apiVersion"], "http.crossplane.io/v1alpha2")

        # ----------------------------
        # Metadata (optional fields)
        # ----------------------------

        metadata = result.get("metadata", {})

        # Metadata may be empty
        self.assertIsInstance(metadata, dict)

        # ----------------------------
        # Spec assertions
        # ----------------------------

        spec = result["spec"]["forProvider"]

        # Headers
        headers = spec["headers"]
        self.assertEqual(
            headers["Authorization"][0],
            "Basic YXJpc3RhOmFyaXN0YQ==",
        )
        self.assertIn("application/json", headers["Accept"])

        # Base URL from environment
        payload = spec["payload"]
        self.assertEqual(
            payload["baseUrl"],
            "http://ceos01.default.svc.cluster.local:6021/command-api",
        )

        # JSON-RPC payload
        body = json.loads(payload["body"])
        self.assertEqual(body["method"], "runCmds")
        self.assertIn(
            "ip prefix-list PL-Loopback0",
            " ".join(body["params"]["cmds"]),
        )

        # ----------------------------
        # Mappings
        # ----------------------------

        actions = {m["action"] for m in spec["mappings"]}
        self.assertSetEqual(
            actions,
            {"CREATE", "UPDATE", "OBSERVE", "REMOVE"},
        )

    async def test_namespaced_composite_gets_namespaced_request(self) -> None:
        """A namespaced composite composes a namespaced Request.

        Crossplane refuses to apply a cluster scoped composed resource for a
        namespaced composite, failing the whole reconcile -- so the API group
        has to follow the composite's scope. The test above covers the cluster
        scoped case, where the composite carries no namespace.
        """

        composite = {
            "apiVersion": "netclab.dev/v1alpha1",
            "kind": "CliConfig",
            "metadata": {"name": "eoscommand-1", "namespace": "lab"},
            "spec": {
                "endpoint": "ceos01.default.svc.cluster.local",
                "cmds": {"ip prefix-list PL-Loopback0": {}},
            },
        }

        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "eos-creds", "namespace": "crossplane-system"},
            "type": "Opaque",
            "data": {"basicAuth": "YXJpc3RhOmFyaXN0YQ=="},
        }

        environment = {
            "restconf": {"scheme": "https", "port": 6020},
            "jsonrpc": {"scheme": "http", "port": 6021},
        }

        req = fnv1.RunFunctionRequest(
            input=resource.dict_to_struct({"version": "v1beta2"}),
            observed=fnv1.State(
                composite=fnv1.Resource(resource=resource.dict_to_struct(composite))
            ),
            required_resources={
                "eos-creds": fnv1.Resources(
                    items=[fnv1.Resource(resource=resource.dict_to_struct(secret))]
                )
            },
            context=structpb.Struct(
                fields={
                    "apiextensions.crossplane.io/environment": structpb.Value(
                        struct_value=resource.dict_to_struct(environment)
                    )
                }
            ),
        )

        resp = await fn.FunctionRunner().RunFunction(req, None)

        self.assertGreater(len(resp.desired.resources), 0, "no desired resources")
        for name, composed in resp.desired.resources.items():
            result = MessageToDict(composed.resource)
            self.assertEqual(
                result["apiVersion"],
                "http.m.crossplane.io/v1alpha2",
                f"{name} is not namespaced",
            )
            self.assertEqual(result["kind"], "Request")
