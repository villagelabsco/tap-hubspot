from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.streams import RESTStream
from typing import Any, Mapping, TypedDict, TypeGuard


class HubSpotOAuthAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    def __init__(self, stream: RESTStream) -> None:
        super().__init__(
            auth_endpoint="https://api.hubapi.com/oauth/v1/token",
            stream=stream,
        )

    @property
    def oauth_request_body(self) -> dict:
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
        }


class OAuthCredentials(TypedDict):
    client_id: str
    client_secret: str
    refresh_token: str


def is_oauth_credentials(value: Mapping[str, Any]) -> TypeGuard[OAuthCredentials]:
    return "refresh_token" in value
