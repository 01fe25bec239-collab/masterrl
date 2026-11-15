"""Golden round-trip on the Python side, plus checks that the goldens and the committed
generated code match proto/. The Rust side is crates/mrl-proto/src/golden.rs.
Regenerate everything with `just gen`.
"""

import importlib
from pathlib import Path
from typing import Any

import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import Descriptor

GOLDEN = Path(__file__).parent / "golden"
FDS = descriptor_pb2.FileDescriptorSet.FromString((GOLDEN / "descriptors.binpb").read_bytes())
MRL_FILES = [f for f in FDS.file if f.package.startswith("mrl.")]
POOL = descriptor_pool.DescriptorPool()
for _file in FDS.file:
    POOL.Add(_file)

GOLDENS = sorted(p.name for p in GOLDEN.glob("*.binpb") if p.name != "descriptors.binpb")
MESSAGES = sorted(f"{f.package}.{m.name}" for f in MRL_FILES for m in f.message_type)

# Generated packages and the proto areas each one holds (see tools/gen/proto.py).
GENERATED = {
    "mrl_runtime._generated": ("common", "worker", "reward", "env"),
    "mrl_rewards._generated": ("common", "reward", "env"),
}


def message_name(golden: str) -> str:
    return ".".join(golden.removesuffix(".binpb").split(".")[:4])


def decode(golden: str) -> Any:
    cls = message_factory.GetMessageClass(POOL.FindMessageTypeByName(message_name(golden)))
    return cls.FromString((GOLDEN / golden).read_bytes())


def unset_fields(msg: Any) -> list[str]:
    """Fields left at their default, outside unselected oneof arms (recursive)."""
    desc: Descriptor = msg.DESCRIPTOR
    chosen = {o.name: msg.WhichOneof(o.name) for o in desc.oneofs}
    missing = []
    for field in desc.fields:
        oneof = field.containing_oneof
        if oneof is not None and chosen[oneof.name] != field.name:
            if chosen[oneof.name] is None:
                missing.append(field.name)
            continue
        value = getattr(msg, field.name)
        if field.is_repeated:
            if not value:
                missing.append(field.name)
            entry = field.message_type
            if entry is not None and entry.GetOptions().map_entry:
                items = value.values() if entry.fields_by_name["value"].message_type else []
            else:
                items = value if entry is not None else []
            missing += [f"{field.name}.{m}" for item in items for m in unset_fields(item)]
        elif field.message_type is not None:
            if not msg.HasField(field.name):
                missing.append(field.name)
            missing += [f"{field.name}.{m}" for m in unset_fields(value)]
        elif value == field.default_value:
            missing.append(field.name)
    return missing


def without_json_names(file: descriptor_pb2.FileDescriptorProto) -> bytes:
    """protoc keeps json_name in descriptor sets but drops it from Python gencode."""
    copy = descriptor_pb2.FileDescriptorProto()
    copy.CopyFrom(file)
    stack = list(copy.message_type)
    while stack:
        message = stack.pop()
        stack += message.nested_type
        for field in message.field:
            field.ClearField("json_name")
    return copy.SerializeToString()


@pytest.mark.parametrize("golden", GOLDENS)
def test_golden_round_trips_byte_for_byte(golden: str) -> None:
    msg = decode(golden)
    assert msg.SerializeToString(deterministic=True) == (GOLDEN / golden).read_bytes()


@pytest.mark.parametrize("golden", GOLDENS)
def test_golden_sets_every_field(golden: str) -> None:
    assert unset_fields(decode(golden)) == []


def test_every_message_has_a_golden_and_every_golden_a_message() -> None:
    assert sorted({message_name(g) for g in GOLDENS}) == MESSAGES


def test_every_oneof_arm_has_a_golden() -> None:
    for name in MESSAGES:
        desc = POOL.FindMessageTypeByName(name)
        real = [
            o for o in desc.oneofs if not (len(o.fields) == 1 and o.name == f"_{o.fields[0].name}")
        ]
        for oneof in real[:1]:
            for field in oneof.fields:
                assert f"{name}.{field.name}.binpb" in GOLDENS


@pytest.mark.parametrize("package", sorted(GENERATED))
def test_generated_code_matches_protos(package: str) -> None:
    for file in MRL_FILES:
        area = file.package.split(".")[1]
        module = f"{package}.{file.name.removesuffix('.proto').replace('/', '.')}_pb2"
        if area not in GENERATED[package]:
            with pytest.raises(ModuleNotFoundError):
                importlib.import_module(module)
            continue
        generated = importlib.import_module(module).DESCRIPTOR
        assert generated.serialized_pb == without_json_names(file), module
        if file.service:
            importlib.import_module(f"{module}_grpc")
