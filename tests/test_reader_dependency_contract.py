from __future__ import annotations

import re
import unittest

from collectors.production_preflight import REPO_ROOT


_NAME_RE = re.compile(r"^([A-Za-z0-9_.-]+)")


def _requirement_names(lines: list[str]) -> set[str]:
    names: set[str] = set()
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _NAME_RE.match(stripped)
        if match:
            names.add(match.group(1).lower().replace("_", "-"))
    return names


class TestReaderDependencyContract(unittest.TestCase):
    def test_reader_input_covers_all_public_wheel_dependencies(self) -> None:
        project_text = (REPO_ROOT / "pyproject.toml").read_text()
        dependency_block = re.search(r"^dependencies\s*=\s*\[(?P<body>.*?)^\]", project_text, flags=re.MULTILINE | re.DOTALL)
        self.assertIsNotNone(dependency_block)
        public_dependencies = _requirement_names(
            [line.strip().strip(",").strip('"') for line in dependency_block.group("body").splitlines()]
        )
        reader_input = _requirement_names((REPO_ROOT / "requirements-reader.in").read_text().splitlines())
        reader_lock = _requirement_names((REPO_ROOT / "requirements-reader.lock").read_text().splitlines())
        self.assertEqual(reader_input, public_dependencies)
        self.assertTrue(public_dependencies.issubset(reader_lock))


if __name__ == "__main__":
    unittest.main()
