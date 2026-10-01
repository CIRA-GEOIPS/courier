from courier.interfaces.payloads import Payload
from courier.plugins.payloads.shell_payload import ShellPayload


class TestRepresentationHierarchy:
    def test_child_hierarchy(self) -> None:
        res = ShellPayload.get_representation_hierarchy()

        assert len(res) == 1
        assert res == [ShellPayload]

    def test_child_of_child_hierarchy(self) -> None:
        class GrandchildPayload(ShellPayload):
            pass

        res = GrandchildPayload.get_representation_hierarchy()
        assert len(res) == 2
        assert res == [ShellPayload, GrandchildPayload]

    def test_base_hierarchy(self) -> None:
        res = Payload.get_representation_hierarchy()

        assert len(res) == 0
        assert res == []
