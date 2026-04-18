from tests.fixtures.factories.workspace import WorkspaceFactory


def test_create_workspace():
    workspace = WorkspaceFactory.build(name="Test Workspace")

    assert workspace.name == "Test Workspace"
