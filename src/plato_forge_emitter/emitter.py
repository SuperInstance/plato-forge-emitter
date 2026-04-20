import dataclasses
import json
import os
import time
from typing import Any, Optional


@dataclasses.dataclass
class Artifact:
    name: str
    version: str
    content: str
    quality_score: float
    metadata: dict
    created_at: float


class ForgeEmitter:
    def __init__(
        self,
        output_dir: str = "./artifacts",
        min_quality: float = 0.5,
        auto_version: bool = True,
    ) -> None:
        self.output_dir = output_dir
        self.min_quality = min_quality
        self.auto_version = auto_version
        os.makedirs(self.output_dir, exist_ok=True)

    def _artifact_path(self, name: str, version: str) -> str:
        filename = f"{name}_v{version}.json"
        return os.path.join(self.output_dir, filename)

    def _next_version(self, name: str) -> str:
        latest = self.latest_version(name)
        if latest is None:
            return "1"
        try:
            return str(int(latest) + 1)
        except ValueError:
            return str(float(latest) + 1.0)

    def quality_gate(self, score: float, min_quality: Optional[float] = None) -> bool:
        threshold = min_quality if min_quality is not None else self.min_quality
        return score >= threshold

    def emit(
        self,
        name: str,
        content: str,
        quality_score: float,
        metadata: Optional[dict] = None,
    ) -> Optional[Artifact]:
        if not self.quality_gate(quality_score):
            return None

        version = self._next_version(name) if self.auto_version else "1"
        artifact = Artifact(
            name=name,
            version=version,
            content=content,
            quality_score=quality_score,
            metadata=metadata or {},
            created_at=time.time(),
        )

        path = self._artifact_path(name, version)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(dataclasses.asdict(artifact), f, indent=2)

        return artifact

    def emit_batch(
        self,
        artifacts: list[dict[str, Any]],
    ) -> list[Artifact]:
        emitted: list[Artifact] = []
        for item in artifacts:
            artifact = self.emit(
                name=item["name"],
                content=item["content"],
                quality_score=item["quality_score"],
                metadata=item.get("metadata", {}),
            )
            if artifact is not None:
                emitted.append(artifact)
        return emitted

    def format_commit_message(self, artifact: Artifact) -> str:
        return (
            f"[I2I:FORGE] {artifact.name} v{artifact.version} "
            f"— quality {artifact.quality_score:.2f}"
        )

    def list_artifacts(self) -> list[Artifact]:
        artifacts: list[Artifact] = []
        if not os.path.isdir(self.output_dir):
            return artifacts

        for filename in os.listdir(self.output_dir):
            if not filename.endswith(".json"):
                continue
            path = os.path.join(self.output_dir, filename)
            if not os.path.isfile(path):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                artifact = Artifact(**data)
                artifacts.append(artifact)
            except (json.JSONDecodeError, TypeError, KeyError):
                continue

        return artifacts

    def latest_version(self, name: str) -> Optional[str]:
        versions: list[str] = []
        prefix = f"{name}_v"
        suffix = ".json"

        if not os.path.isdir(self.output_dir):
            return None

        for filename in os.listdir(self.output_dir):
            if filename.startswith(prefix) and filename.endswith(suffix):
                version_part = filename[len(prefix) : -len(suffix)]
                versions.append(version_part)

        if not versions:
            return None

        def _sort_key(v: str) -> tuple:
            try:
                return (0, float(v))
            except ValueError:
                return (1, v)

        versions.sort(key=_sort_key)
        return versions[-1]

    def stats(self) -> dict[str, Any]:
        artifacts = self.list_artifacts()
        total = len(artifacts)
        by_name: dict[str, int] = {}
        quality_sum = 0.0

        for artifact in artifacts:
            by_name[artifact.name] = by_name.get(artifact.name, 0) + 1
            quality_sum += artifact.quality_score

        avg_quality = quality_sum / total if total > 0 else 0.0

        return {
            "total_emitted": total,
            "by_name": by_name,
            "avg_quality": avg_quality,
        }
