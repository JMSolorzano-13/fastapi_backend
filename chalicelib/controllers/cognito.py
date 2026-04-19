import jwt
import requests
from chalice import UnauthorizedError
from jwt import PyJWKClient

from chalicelib.new.config.infra import envars

# JWKS only when validating real Cognito RS256 tokens (not LOCAL_INFRA lax mode, not local_jwt POC)
_use_cognito_jwks = (
    not envars.LOCAL_INFRA
    and envars.AUTH_BACKEND == "cognito"
    and bool(envars.COGNITO_USER_POOL_ID)
)
if _use_cognito_jwks:
    jwks_url = (
        f"https://cognito-idp.{envars.REGION_NAME}.amazonaws.com/"
        f"{envars.COGNITO_USER_POOL_ID}/.well-known/jwks.json"
    )
    jwks_client = PyJWKClient(jwks_url)
else:
    jwks_client = None


def get_signing_key(id_token: str):
    if jwks_client is None:
        raise RuntimeError("JWKS client not initialized (LOCAL_INFRA mode)")
    return jwks_client.get_signing_key_from_jwt(id_token).key


def get_options() -> dict:
    return {}


def decode_token(id_token: str):
    """
    Decode and validate a JWT token.

    In LOCAL_INFRA mode, skips signature verification to allow local development
    without Cognito. In production mode, validates against Cognito JWKS.

    Args:
        id_token: JWT token string

    Returns:
        Decoded token payload

    Raises:
        UnauthorizedError: If token is invalid
    """
    if envars.AUTH_BACKEND == "local_jwt":
        from chalicelib.new.config.infra import local_auth

        try:
            return local_auth.decode_local_jwt(id_token, verify_signature=True)
        except Exception as e:
            raise UnauthorizedError(f"Token validation failed: {str(e)}") from e

    # Local development mode: skip signature verification
    if envars.LOCAL_INFRA:
        try:
            # Decode without signature verification
            return jwt.decode(
                id_token,
                options={"verify_signature": False, "verify_exp": False, "verify_aud": False},
            )
        except Exception as e:
            raise UnauthorizedError(f"Invalid token format: {str(e)}") from e

    # Production mode: full validation with Cognito JWKS
    try:
        signing_key = get_signing_key(id_token)
        return jwt.decode(
            id_token,
            signing_key,
            algorithms=["RS256"],
            audience=envars.COGNITO_CLIENT_ID,
            issuer=f"https://cognito-idp.{envars.REGION_NAME}.amazonaws.com/{envars.COGNITO_USER_POOL_ID}",
            options=get_options(),
        )
    except Exception as e:
        raise UnauthorizedError(f"Token validation failed: {str(e)}") from e


def exchange_code_for_tokens(code: str):
    """
    Intercambia un código de autorización por tokens de acceso e ID.
    """

    if envars.AUTH_BACKEND == "local_jwt":
        raise UnauthorizedError(
            "OAuth authorization code exchange is not enabled when AUTH_BACKEND=local_jwt"
        )

    # url = https://solucioncp-version-sg-sgdev.auth.us-east-1.amazoncognito.com/oauth2/token
    # https://solucioncp-version-sg-sgdev.auth.us-east-1.amazoncognito.com
    url = f"{envars.COGNITO_URL}/oauth2/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "client_id": envars.COGNITO_CLIENT_ID,
        "code": code,
        "redirect_uri": envars.COGNITO_REDIRECT_URI,
    }
    if envars.COGNITO_CLIENT_SECRET:
        data["client_secret"] = envars.COGNITO_CLIENT_SECRET

    response = requests.post(url, headers=headers, data=data)

    if response.status_code != 200:
        info = response.json()
        info["probable_cause"] = "Maybe the code has already been used or is invalid."
        raise UnauthorizedError(info)

    return response.json()
