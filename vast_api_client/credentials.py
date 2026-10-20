from pydantic import BaseModel, ConfigDict, SecretStr, field_validator, model_validator


class VastCredentials(BaseModel):
    """
    How to authenticate to the API: a user and password (HTTP basic auth) or an API token, never both.
    Checked here, before anything reaches vastpy, so a bad combination fails with a clear message.
    Secrets are SecretStr and inputs are hidden from validation errors, so neither shows up in reprs or logs.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    user: str | None = None
    password: SecretStr | None = None
    token: SecretStr | None = None
    tenant: str | None = None

    @field_validator("*", mode="before")
    @classmethod
    def blank_means_missing(cls, value):
        # an empty string from an unset env var or config entry counts as not supplied
        return None if value == "" else value

    @model_validator(mode="after")
    def check_auth_method(self):
        has_user = self.user is not None
        has_password = self.password is not None
        if self.token is not None:
            if has_user or has_password:
                raise ValueError("supply either a user and password or a token, not both")
            return self
        if has_user and not has_password:
            raise ValueError("a user was supplied without a password")
        if has_password and not has_user:
            raise ValueError("a password was supplied without a user")
        if not has_user:
            raise ValueError("no credentials: supply a user and password, or a token")
        return self
