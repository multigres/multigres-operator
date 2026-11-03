#!/usr/bin/env python3
"""Reject incomplete Syft SPDX inventories of the released operator image."""

import json
import re
import sys


def check(document):
    def require(condition, message):
        if not condition:
            raise ValueError(message)

    require(document.get("spdxVersion") == "SPDX-2.3", "expected SPDX 2.3")
    require(document.get("SPDXID") == "SPDXRef-DOCUMENT", "missing document ID")
    packages = document.get("packages", [])
    files = document.get("files", [])
    elements = [document, *packages, *files]
    ids = [element.get("SPDXID") for element in elements]
    require(all(ids) and len(ids) == len(set(ids)), "missing or duplicate SPDX IDs")
    relationships = document.get("relationships", [])
    require(relationships, "missing package relationships")
    for relation in relationships:
        require(relation.get("spdxElementId") in ids, "unknown relationship source")
        require(relation.get("relatedSpdxElement") in ids, "unknown relationship target")

    for binary in ("manager", "multigres-gc"):
        matches = [file for file in files if file.get("fileName", "").lstrip("/") == binary]
        require(len(matches) == 1, f"missing or duplicate {binary} binary")
        require(any(c.get("algorithm") == "SHA256" and
                    re.fullmatch(r"[0-9a-f]{64}", c.get("checksumValue", ""))
                    for c in matches[0].get("checksums", [])), f"missing {binary} checksum")
        # Syft's sourceInfo identifies the binary whose Go buildinfo supplied
        # these packages. Checking both prevents a one-binary inventory passing.
        modules = [package for package in packages
                   if package.get("sourceInfo", "").endswith(f": /{binary}")]
        names = {package.get("name") for package in modules}
        require("github.com/multigres/multigres-operator" in names,
                f"missing {binary} main module")
        require("stdlib" in names, f"missing {binary} Go standard library")
        require(any(name and name.startswith("github.com/") and
                    name != "github.com/multigres/multigres-operator" for name in names),
                f"missing {binary} embedded dependencies")

    require(any(ref.get("referenceType") == "purl" and
                ref.get("referenceLocator", "").startswith("pkg:deb/")
                for package in packages for ref in package.get("externalRefs", [])),
            "missing distroless OS packages")


if __name__ == "__main__":
    try:
        with open(sys.argv[1], encoding="utf-8") as source:
            check(json.load(source))
    except (ValueError, KeyError, TypeError, OSError) as error:
        sys.exit(f"Invalid release SBOM: {error}")
    print("Release SBOM contains both binaries, Go dependencies, and OS packages")
