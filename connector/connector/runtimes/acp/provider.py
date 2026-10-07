"""AcpProvider: discovery, config schema, validation, and creation."""

from __future__ import annotations

import os
import shutil
from collections.abc import Mapping
from typing import Any

from jsonschema import Draft202012Validator

from connector.runtime_protocol import (
    AgentRuntime,
    RuntimeConfig,
    RuntimeConfigSchema,
    RuntimeInstancePolicy,
    RuntimeInvalidRequestError,
    RuntimeProvider,
    RuntimeTypeDescriptor,
)
from connector.runtime_protocol.protocol import AgentRuntime
from connector.runtime_protocol.host import RuntimeHostClient
from connector.runtimes.acp.runtime import AcpRuntime

ACP_CONFIG_SCHEMA_REVISION = 1


def acp_config_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "command": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
                "minItems": 1,
                "title": "ACP agent command",
                "description": (
                    "Command line that starts the ACP agent, e.g. "
                    '["hermes", "acp"] or ["openclaw", "acp", "--token", "..."].'
                ),
            },
            "cwd": {
                "type": "string",
                "minLength": 1,
                "title": "Working directory",
                "description": "Working directory for the ACP agent process.",
            },
            "displayName": {
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "title": "Display name",
                "description": "Shown in the Agents Anywhere UI, e.g. 'Hermes (default)'.",
            },
            "environment": {
                "type": "object",
                "title": "Environment variables",
                "propertyNames": {"pattern": "^[^=\\u0000]+$"},
                "additionalProperties": {"type": "string"},
                "default": {},
            },
        },
        "required": ["command", "displayName"],
        "additionalProperties": False,
    }


def acp_capabilities() -> dict[str, bool]:
    return {
        "modelCatalog": False,
        "permissionCatalog": False,
        "sessionDiscovery": True,
        "sessionSnapshot": True,
        "sessionState": True,
        "sessionNotices": True,
        "createAndStartSession": True,
        "startTurn": True,
        "steerTurn": False,
        "interruptTurn": True,
        "commands": False,
        "interactions": True,
        "attachments": False,
        "ipc": False,
    }


class AcpProvider(RuntimeProvider):
    @property
    def runtime(self) -> str:
        return "acp"

    @property
    def runtime_type(self) -> str:
        return "acp"

    @property
    def display_name(self) -> str:
        return "ACP Agent"

    @property
    def description(self) -> str:
        return "Agent Client Protocol runtime (hermes acp / openclaw acp / any ACP agent)"

    @property
    def implementation_type(self) -> str | None:
        return "acp-subprocess"

    @property
    def instance_policy(self) -> RuntimeInstancePolicy:
        return "multiple"

    @property
    def max_instances(self) -> int | None:
        return None

    async def discover(self) -> RuntimeTypeDescriptor:
        return RuntimeTypeDescriptor(
            runtime_type=self.runtime_type,
            display_name=self.display_name,
            description=self.description,
            available=True,
            capabilities=acp_capabilities(),
            reason=None,
            config_schema=await self.get_config_schema(),
            instance_policy=self.instance_policy,
            max_instances=self.max_instances,
            recommended=False,
            metadata={
                "configured": True,
                "protocol": "acp-1",
            },
        )

    async def get_config_schema(self) -> RuntimeConfigSchema:
        return RuntimeConfigSchema(
            runtime=self.runtime,
            revision=ACP_CONFIG_SCHEMA_REVISION,
            schema=acp_config_schema(),
            ui_schema={
                "order": ["displayName", "command", "cwd", "environment"],
                "environment": {"component": "keyValue"},
            },
            defaults={
                "command": ["hermes", "acp"],
                "cwd": "/home/zjx",
                "displayName": "Hermes (default)",
                "environment": {},
            },
        )

    async def validate_config(
        self,
        values: Mapping[str, Any],
    ) -> RuntimeConfig:
        raw_values = dict(values)
        schema = (await self.get_config_schema()).schema
        errors = sorted(
            Draft202012Validator(schema).iter_errors(raw_values),
            key=lambda error: list(error.absolute_path),
        )
        if errors:
            path = "/" + "/".join(str(part) for part in errors[0].absolute_path)
            raise RuntimeInvalidRequestError(
                f"acp config is invalid at {path or '/'}: {errors[0].message}"
            )

        command = [str(part) for part in raw_values["command"]]
        executable = shutil.which(command[0]) or (
            command[0]
            if command[0].startswith("/")
            and os.path.isfile(os.path.expanduser(command[0]))
            and os.access(os.path.expanduser(command[0]), os.X_OK)
            else None
        )
        if executable is None:
            raise RuntimeInvalidRequestError(
                f"ACP agent executable {command[0]!r} was not found"
            )
        cwd = os.path.expanduser(str(raw_values.get("cwd") or "/"))
        if not os.path.isdir(cwd):
            raise RuntimeInvalidRequestError(
                f"ACP working directory {cwd!r} does not exist"
            )

        normalized: dict[str, Any] = {
            "command": command,
            "displayName": str(raw_values["displayName"]),
            "cwd": cwd,
            "environment": dict(raw_values.get("environment") or {}),
        }
        return RuntimeConfig(
            runtime=self.runtime,
            revision=ACP_CONFIG_SCHEMA_REVISION,
            values=normalized,
            schema=schema,
            ui_schema=(await self.get_config_schema()).ui_schema,
            metadata={"protocol": "acp-1", "executable": executable},
        )

    async def create_runtime(
        self,
        config: RuntimeConfig,
        host: RuntimeHostClient,
    ) -> AgentRuntime:
        runtime: AgentRuntime = AcpRuntime(config=config, host=host)
        return runtime
