from chalicelib.boto3_clients import cognito_client
from chalicelib.controllers.user import _admin_create_or_get_cognito_user
from chalicelib.new.config.infra import envars


def test_admin_create_or_get_cognito_user():
    email = "test@test.com"
    _admin_create_or_get_cognito_user(email)
    user_cognito = cognito_client().admin_get_user(
        UserPoolId=envars.COGNITO_USER_POOL_ID, Username=email
    )
    assert user_cognito


def test_admin_create_or_get_cognito_user_duplicated_entry():
    email = "test@test.com"
    _admin_create_or_get_cognito_user(email)
    _admin_create_or_get_cognito_user(email)
    user_cognito = cognito_client().admin_get_user(
        UserPoolId=envars.COGNITO_USER_POOL_ID, Username=email
    )
    assert user_cognito
