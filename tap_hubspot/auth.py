from functools import cached_property
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.streams import RESTStream
from typing import Any, Mapping, TypedDict, TypeGuard
import os

class HubSpotOAuthAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    def __init__(self, stream: RESTStream) -> None:
        super().__init__(
            auth_endpoint="https://api.hubapi.com/oauth/v1/token",
            stream=stream,
        )

    @cached_property
    def oauth_request_body(self) -> dict:
        env_client_id = os.getenv("TAP_HUBSPOT_CLIENT_ID")
        env_client_secret = os.getenv("TAP_HUBSPOT_CLIENT_SECRET")
        env_refresh_token = os.getenv("TAP_HUBSPOT_REFRESH_TOKEN")
        return {
            "client_id": self.config.get("client_id") or env_client_id,
            "client_secret": self.config.get("client_secret") or env_client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.config.get("refresh_token") or env_refresh_token,
        }


class OAuthCredentials(TypedDict):
    client_id: str
    client_secret: str
    refresh_token: str


def is_oauth_credentials(value: Mapping[str, Any]) -> TypeGuard[OAuthCredentials]:
    return "refresh_token" in value
