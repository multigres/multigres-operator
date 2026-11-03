"""The release gate must reject structurally valid but incomplete inventories."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


def inventory():
    document = {"spdxVersion": "SPDX-2.3", "SPDXID": "SPDXRef-DOCUMENT",
                "packages": [], "files": [], "relationships": []}
    for binary in ("manager", "multigres-gc"):
        document["files"].append({"SPDXID": f"SPDXRef-{binary}", "fileName": binary,
                                  "checksums": [{"algorithm": "SHA256", "checksumValue": "a" * 64}]})
        for index, name in enumerate(("github.com/multigres/multigres-operator", "stdlib", "github.com/go-logr/logr")):
            document["packages"].append({"SPDXID": f"SPDXRef-{binary}-{index}", "name": name,
                                         "sourceInfo": f"acquired package info from go module information: /{binary}"})
    document["packages"].append({"SPDXID": "SPDXRef-base", "name": "base-files",
                                 "externalRefs": [{"referenceType": "purl", "referenceLocator": "pkg:deb/debian/base-files@13"}]})
    for element in document["packages"] + document["files"]:
        document["relationships"].append({"spdxElementId": "SPDXRef-DOCUMENT",
                                           "relatedSpdxElement": element["SPDXID"], "relationshipType": "CONTAINS"})
    return document


class ReleaseSBOMTest(unittest.TestCase):
    def run_check(self, document):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sbom.json"
            path.write_text(json.dumps(document))
            return subprocess.run([sys.executable, str(Path(__file__).with_name("check-release-sbom.py")), str(path)],
                                  capture_output=True, text=True)

    def test_complete_inventory(self):
        result = self.run_check(inventory())
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_incomplete_and_broken_inventories(self):
        original = inventory()
        cases = {}
        for binary in ("manager", "multigres-gc"):
            missing = copy.deepcopy(original)
            # Keep all IDs and relationships valid: an unrelated file or module
            # must not satisfy the expected application coverage.
            next(f for f in missing["files"] if f["fileName"] == binary)["fileName"] = "other"
            cases[f"missing {binary}"] = missing
            missing = copy.deepcopy(original)
            for package in missing["packages"]:
                if package.get("sourceInfo", "").endswith(f"/{binary}"):
                    package["sourceInfo"] = "not from the shipped binary"
            cases[f"missing {binary} dependencies"] = missing
        missing = copy.deepcopy(original)
        missing["packages"][-1]["externalRefs"] = []
        cases["missing OS inventory"] = missing
        broken = copy.deepcopy(original)
        broken["relationships"][0]["relatedSpdxElement"] = "SPDXRef-absent"
        cases["dangling relationship"] = broken
        duplicate = copy.deepcopy(original)
        duplicate["packages"].append(duplicate["packages"][0])
        cases["duplicate identity"] = duplicate
        missing = copy.deepcopy(original)
        missing["files"][0]["checksums"] = []
        cases["missing binary digest"] = missing
        cases["empty document"] = {}
        for name, document in cases.items():
            with self.subTest(name=name):
                result = self.run_check(document)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Invalid release SBOM", result.stderr)


if __name__ == "__main__":
    unittest.main()
