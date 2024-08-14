from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream


class HubSpotOAuthAuthenticator(OAuthAuthenticator):
    def __init__(self, stream: RESTStream) -> None:
        super().__init__(
            auth_endpoint="https://api.hubapi.com/oauth/v1/token",
            stream=stream,
        )

    @property
    def oauth_request_body(self) -> dict:
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config.get("client_secret"),
            "grant_type": "refresh_token",
            "refresh_token": self.config.get("refresh_token"),
        }
