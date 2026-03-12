---
role: assistant
timestamp: 2026-03-12T00:05:00Z
session: ci-fix
sequence: 1
---

# Versioned Plugin API CI Fix

Updated test_empty_plugins_list assertion from exact dict match to data["plugins"] == [] since the endpoint now returns additional api_version and dependency_graph fields.
