from tests.fixtures.factories.user import UserFactory


def test_create_user():
    user = UserFactory.build(name="Test User", email="test@example.com")

    assert user.name == "Test User"
    assert user.email == "test@example.com"
