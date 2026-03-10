from __future__ import annotations

from copy import deepcopy as cp
from typing import Any, Awaitable, Callable, Dict, Mapping
from fastapi import FastAPI, HTTPException

Handler = Callable[[Dict[str, Any]], Awaitable[Any]]


def _as_dict_payload(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail=f"Request body must be object/dict, got {type(data)}")
    return data


def _normalize_optional_defaults(optional_keys: Mapping[str, Any] | list[str] | tuple[str, ...] | None) -> Dict[str, Any]:
    if optional_keys is None:
        return {}
    if isinstance(optional_keys, Mapping):
        return dict(optional_keys)
    return {key: None for key in optional_keys}

ENDPOINTS: Dict[str, Any] = {}
def register(
    name: str,
    method: str,
    required_keys: list[str] | tuple[str, ...],
    optional_keys: Mapping[str, Any] | list[str] | tuple[str, ...] | None = None,
    summary: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
):
    """Register an async handler and expose it as given method /api/{name}."""
    required = set(required_keys)
    optional_defaults = _normalize_optional_defaults(optional_keys)

    def decorator(func: Handler) -> Handler:
        async def wrapped(data: Dict[str, Any]):
            payload = _as_dict_payload(data)

            missing = sorted([key for key in required if key not in payload])
            if missing:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "status": "missing_keys",
                        "missing": missing,
                        "required": sorted(required),
                        "given": sorted(payload.keys()),
                    },
                )

            prepared = cp(payload)
            for key, default in optional_defaults.items():
                prepared.setdefault(key, cp(default))

            return await func(prepared)
        
        ENDPOINTS[name] = {
            "method": method.upper(),
            "required_keys": required,
            "optional_defaults": optional_defaults,
            "handler": wrapped,
            "summary": summary,
            "description": description,
            "tags": tags,
        }
        return wrapped
    return decorator


