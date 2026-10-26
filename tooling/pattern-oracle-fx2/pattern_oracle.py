#!/usr/bin/env python3
"""Round-2 EventBridge pattern-matcher oracle matrix (fx2 lane).

Three sides per cell:
  * moto-stock — the pinned moto 5.2.4 fork's EventPattern (in-process,
    the canonical oracle per AGENTS.md).
  * moto-merged — stock + the upstream-merged PR #10360 shim (a
    subclass implementing the PR's per-element array walk), used for the
    "upstream-merged" expectation of the reviewer's matrix.
  * twin — the Rust twin's TestEventPattern over HTTP (exercises request
    parse -> validate -> matcher end to end).

Report prints per-cell values plus final mismatch counts:
  twin vs moto-merged = the headline number (must be 0 modulo the
  pre-pinned divergences tagged in CELLS); twin vs moto-stock shows the
  PR #10360 delta separately.
"""

import json
import os
import sys
import urllib.request

from moto.events.models import EventPattern, UNDEFINED

TWIN_URL = os.environ.get("TWIN_URL", "http://127.0.0.1:4676/")


class MergedEventPattern(EventPattern):
    """moto EventPattern + upstream PR #10360 ("EventBridge: match event
    patterns against arrays of objects"): when a nested pattern asks for
    a path whose flattened value is a list, the pattern is applied to
    each OBJECT of the list (looked through for arrays-of-arrays) and
    matches if one object satisfies all of it — fields never mix across
    elements."""

    def _does_event_match(self, event_flat, pattern_node, prefix=""):
        if not isinstance(pattern_node, dict):
            return False
        results = []
        for k, v in pattern_node.items():
            if k == "$or":
                results.append(
                    any(self._does_event_match(event_flat, p, prefix) for p in v)
                )
            else:
                new_prefix = f"{prefix}.{k}" if prefix else k
                if isinstance(v, dict):
                    item = event_flat.get(new_prefix, UNDEFINED)
                    if isinstance(item, list):
                        results.append(
                            any(
                                self._does_event_match(self._flatten_dict(obj), v)
                                for obj in self._objects_in_array(item)
                            )
                        )
                        continue
                    results.append(self._does_event_match(event_flat, v, new_prefix))
                elif isinstance(v, list):
                    item = event_flat.get(new_prefix, UNDEFINED)
                    results.append(self._does_item_match_filters(item, v))
                else:
                    results.append(False)
        return all(results)

    @staticmethod
    def _objects_in_array(arr):
        for item in arr:
            if isinstance(item, dict):
                yield item
            elif isinstance(item, list):
                yield from MergedEventPattern._objects_in_array(item)


def moto_match(pattern, event, merged):
    cls = MergedEventPattern if merged else EventPattern
    ep = cls.__new__(cls)
    EventPattern.__init__(ep, json.dumps(pattern), pattern)
    try:
        return ep.matches_event(json.loads(event) if isinstance(event, str) else event)
    except IndexError:
        return "crash"


def twin_match(pattern: str, event: str) -> bool:
    body = json.dumps({"EventPattern": pattern, "Event": event}).encode()
    req = urllib.request.Request(
        TWIN_URL + "/",
        data=body,
        headers={
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": "AWSEvents.TestEventPattern",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)["Result"]


def C(label, pattern, event, tag=None):
    return {"label": label, "pattern": pattern, "event": event, "tag": tag}


CELLS = []
A = CELLS.append

# -- G1: scalar ancestors under nested patterns (fix 1, HIGH) ---------------
for scalar in ['"x"', "5", "true"]:
    ev = '{"detail": %s}' % scalar
    A(C(f"scalar-anc exists:false ({scalar})", {"detail": {"state": [{"exists": False}]}}, ev))
    A(C(f"scalar-anc exists:true ({scalar})", {"detail": {"state": [{"exists": True}]}}, ev))
A(C("scalar-anc value list fails", {"detail": {"state": ["running"]}}, '{"detail": "x"}'))
A(C("scalar-anc named filter fails", {"detail": {"state": [{"prefix": "run"}]}}, '{"detail": "running"}'))
A(C("deep scalar-anc exists:false", {"detail": {"a": {"b": [{"exists": False}]}}}, '{"detail": "x"}'))
A(C("deep scalar-anc exists:true", {"detail": {"a": {"b": [{"exists": True}]}}}, '{"detail": "x"}'))
A(C("object-anc exists:false still matched", {"detail": {"state": [{"exists": False}]}}, '{"detail": {"state": "running"}}'))
A(C("absent-key escape hatch intact", {"detail": {"state": [{"exists": False}]}}, "{}"))
A(C("array-anc exists:false (pr10360 shape)", {"detail": {"state": [{"exists": False}]}}, '{"detail": [1, 2]}', tag="pr10360"))
A(C("array-anc w/ object element", {"detail": {"state": [{"exists": False}]}}, '{"detail": [{"state": "x"}]}', tag="pr10360"))

# -- G2: exists vs dict intermediates (fix 2, MED) ---------------------------
B_NEST = {"detail": {"state": [{"exists": True}]}}
B_FNEST = {"detail": {"state": [{"exists": False}]}}
A(C("dict interm nested eT", B_NEST, '{"detail": {"state": {"a": 1}}}'))
A(C("dict interm nested eF", B_FNEST, '{"detail": {"state": {"a": 1}}}'))
A(C("dict interm top eT", {"detail": [{"exists": True}]}, '{"detail": {"a": 1}}'))
A(C("dict interm top eF", {"detail": [{"exists": False}]}, '{"detail": {"a": 1}}'))
A(C("dict interm deep walk eT", {"detail": {"state": {"a": [{"exists": True}]}}}, '{"detail": {"state": {"a": {"b": 1}}}}'))
A(C("dict interm deep walk eF", {"detail": {"state": {"a": [{"exists": False}]}}}, '{"detail": {"state": {"a": {"b": 1}}}}'))
A(C("empty dict interm eT", B_NEST, '{"detail": {"state": {}}}'))
A(C("empty dict interm eF", B_FNEST, '{"detail": {"state": {}}}'))
A(C("empty dict top eT", {"detail": [{"exists": True}]}, '{"detail": {}}'))
A(C("empty dict top eF", {"detail": [{"exists": False}]}, '{"detail": {}}'))
A(C("scalar present eT", B_NEST, '{"detail": {"state": "on"}}'))
A(C("null present eT", B_NEST, '{"detail": {"state": null}}'))
A(C("null present eF", B_FNEST, '{"detail": {"state": null}}'))
A(C("list present eT", B_NEST, '{"detail": {"state": [1, 2]}}'))
A(C("list present eF", B_FNEST, '{"detail": {"state": [1, 2]}}'))
A(C("absent-tree eF", B_FNEST, "{}"))
A(C("null-anc eT", B_NEST, '{"detail": null}'))
A(C("null-anc eF", B_FNEST, '{"detail": null}'))
A(C("dict interm w/ sibling value filter", {"detail": {"state": [{"exists": True}], "code": "200"}}, '{"detail": {"state": {"a": 1}}}'))

# -- G3: first-key rule (fix 3, LOW) -----------------------------------------
A(C("prefix-first vs numeric-2nd", {"count": [{"prefix": "1", "numeric": [">", 5]}]}, '{"count": "100"}'))
A(C("numeric-first prefix-2nd miss", {"count": [{"numeric": [">", 5], "prefix": "9"}]}, '{"count": "99"}'))
A(C("numeric-first vs non-number", {"count": [{"numeric": [">", 5], "prefix": "9"}]}, '{"count": "abc"}'))
A(C("numeric-only control", {"count": [{"numeric": [">", 5]}]}, '{"count": 10}'))
A(C("numeric-first hit", {"count": [{"numeric": [">", 5], "prefix": "9"}]}, '{"count": 10}'))
A(C("exists 2nd position ignored", {"detail": [{"prefix": "d", "exists": False}]}, '{"detail": "dog"}'))
A(C("exists first wins", {"detail": [{"exists": False, "prefix": "d"}]}, '{"detail": "dog"}'))
A(C("suffix-first hit", {"source": [{"suffix": ".png", "prefix": "x"}]}, '{"source": "pic.png"}'))
A(C("suffix-first miss", {"source": [{"suffix": ".gif", "prefix": "pic."}]}, '{"source": "pic.png"}'))
A(C("single-key prefix control", {"source": [{"prefix": "my."}]}, '{"source": "my.bucket"}'))
A(C("bogus filter name", {"source": [{"bogus": "v"}]}, '{"source": "my.bucket"}', tag="bogus-name"))
A(C("empty leaf {}", {"detail": [{}]}, '{"detail": "x"}', tag="moto-crash"))

# -- G4: $or ------------------------------------------------------------------
A(C("$or any exists:false", {"detail": {"$or": [{"state": [{"exists": False}]}, {"other": [{"exists": True}]}]}}, "{}"))
A(C("$or all value filters absent", {"detail": {"$or": [{"state": ["on"]}, {"other": ["off"]}]}}, "{}"))
A(C("$or absent removes key", {"$or": [{"detail": {"state": [{"exists": False}]}}, {"other": "x"}]}, "{}"))
A(C("$or one arm hits", {"$or": [{"source": ["a"]}, {"source": ["b"]}]}, '{"source": "b"}'))
A(C("$or none hits", {"$or": [{"source": ["a"]}, {"source": ["b"]}]}, '{"source": "c"}'))
A(C("$or scalar-anc arm", {"$or": [{"detail": {"state": [{"exists": False}]}}, {"source": ["app"]}]}, '{"detail": "x"}'))
A(C("$or with sibling key", {"$or": [{"source": ["a"]}, {"source": ["b"]}], "detail": {"state": [{"exists": False}]}}, '{"detail": "x"}'))
A(C("$or arm w/ dict interm", {"$or": [{"detail": [{"exists": False}]}, {"source": ["a"]}]}, '{"detail": {"a": 1}}'))

# -- G5: numeric folding -------------------------------------------------------
A(C("int pattern float value", {"count": [2]}, '{"count": 2.0}'))
A(C("float pattern int value", {"count": [2.0]}, '{"count": 2}'))
A(C("membership folds", {"count": [2]}, '{"count": [1, 2.0]}'))
A(C("mismatch magnitude", {"count": [2]}, '{"count": 3.0}'))
A(C("membership miss", {"count": [2]}, '{"count": [1, 3.0]}'))
A(C("string never folds", {"count": [2]}, '{"count": "2"}'))
A(C("bool-vs-int python folds", {"count": [1]}, '{"count": true}', tag="bool-fold"))
A(C("anything-but nr fold excl", {"count": [{"anything-but": 2}]}, '{"count": 2.0}'))
A(C("anything-but nr fold pass", {"count": [{"anything-but": 2}]}, '{"count": 3.0}'))
A(C("numeric gt fold", {"count": [{"numeric": [">", 1]}]}, '{"count": 2.0}'))
A(C("numeric int/float swap", {"count": [{"numeric": [">", 1.5]}]}, '{"count": 2}'))

# -- G6: array-of-objects (moto PR #10360, shim side) --------------------------
A(C("aoo basic", {"detail": {"items": {"id": ["i-2"]}}}, '{"detail": {"items": [{"id": "i-1"}, {"id": "i-2"}]}}', tag="pr10360"))
A(C("aoo no-match", {"detail": {"items": {"id": ["i-3"]}}}, '{"detail": {"items": [{"id": "i-1"}, {"id": "i-2"}]}}', tag="pr10360"))
A(C("aoo single object", {"detail": {"items": {"id": ["i-2"]}}}, '{"detail": {"items": {"id": "i-2"}}}', tag="pr10360"))
A(C("aoo per-object no mix", {"detail": {"employees": {"firstName": ["Anna"], "lastName": ["Smith"]}}}, '{"detail": {"employees": [{"firstName": "John", "lastName": "Smith"}, {"firstName": "Anna", "lastName": "Doe"}]}}', tag="pr10360"))
A(C("aoo per-object both in one", {"detail": {"employees": {"firstName": ["Anna"], "lastName": ["Smith"]}}}, '{"detail": {"employees": [{"firstName": "Anna", "lastName": "Smith"}]}}', tag="pr10360"))
A(C("aoo multi-level arrays", {"detail": {"containers": {"state": {"exitCode": [{"numeric": [">", 0]}]}}}}, '{"detail": {"containers": [[{"state": {"exitCode": 0}}], [{"state": {"exitCode": 137}}]]}}', tag="pr10360"))
A(C("aoo all below threshold", {"detail": {"containers": {"state": {"exitCode": [{"numeric": [">", 0]}]}}}}, '{"detail": {"containers": [{"state": {"exitCode": 0}}]}}', tag="pr10360"))
A(C("aoo empty array", {"detail": {"containers": {"state": {"exitCode": [{"numeric": [">", 0]}]}}}}, '{"detail": {"containers": []}}', tag="pr10360"))
A(C("aoo scalar array", {"detail": {"containers": {"state": {"exitCode": [{"numeric": [">", 0]}]}}}}, '{"detail": {"containers": ["a", "b"]}}', tag="pr10360"))
A(C("aoo equals leaf", {"detail": {"items": {"kind": ["snapshot"]}}}, '{"detail": {"items": [{"kind": "snapshot"}, {"kind": "backup"}]}}', tag="pr10360"))
A(C("aoo nested arrays mixed", {"detail": {"items": {"id": ["x"]}}}, '{"detail": {"items": [[{"id": "y"}], {"id": "x"}]}}', tag="pr10360"))
A(C("aoo whole-array equality", {"detail": {"tags": ["a", "c"]}}, '{"detail": {"tags": ["a", "c"]}}', tag="pr10360"))
A(C("aoo whole-array equality partial", {"detail": {"tags": ["a"]}}, '{"detail": {"tags": ["a", "c"]}}', tag="pr10360"))

# -- G7: classic leaves (control set) ------------------------------------------
A(C("scalar equality", {"source": ["my.app"]}, '{"source": "my.app"}'))
A(C("scalar equality miss", {"source": ["my.app"]}, '{"source": "your.app"}'))
A(C("element in list value", {"detail": {"codes": [200]}}, '{"detail": {"codes": [200, 404]}}'))
A(C("prefix string", {"source": [{"prefix": "my."}]}, '{"source": "my.app"}'))
A(C("suffix list", {"source": [{"suffix": [".png", ".jpg"]}]}, '{"source": "pic.jpg"}'))
A(C("equals-ignore-case", {"source": [{"equals-ignore-case": "My.App"}]}, '{"source": "MY.APP"}'))
A(C("numeric pairs", {"count": [{"numeric": [">", 1, "<", 5]}]}, '{"count": 3}'))
A(C("numeric pairs fail 2nd", {"count": [{"numeric": [">", 1, "<", 5]}]}, '{"count": 9}'))
A(C("anything-but scalar", {"source": [{"anything-but": "a"}]}, '{"source": "b"}'))
A(C("anything-but list excl", {"source": [{"anything-but": ["a", "b"]}]}, '{"source": "a"}'))
A(C("cidr hit", {"detail": {"ip": [{"cidr": "203.0.113.0/24"}]}}, '{"detail": {"ip": "203.0.113.9"}}'))
A(C("wildcard hit", {"detail": {"path": [{"wildcard": "/tmp/*"}]}}, '{"detail": {"path": "/tmp/app"}}'))
A(C("equals-ignore-case value in prefix", {"detail": {"kind": [{"prefix": {"equals-ignore-case": "CREAT"}}]}}, '{"detail": {"kind": "created"}}'))


def main():
    merged_mismatch = []
    stock_mismatch = []
    per_tag = {}
    for cell in CELLS:
        p, e = cell["pattern"], cell["event"]
        p_str = json.dumps(p)
        stock = moto_match(p, e, merged=False)
        merged = moto_match(p, e, merged=True)
        twin = twin_match(p_str, e)
        cell["stock"], cell["merged"], cell["twin"] = stock, merged, twin
        tag = cell["tag"] or ""
        per_tag.setdefault(tag, [0, 0])
        if merged != twin:
            merged_mismatch.append(cell)
            if tag:
                per_tag[tag][0] += 1
        else:
            per_tag[tag][1] += 1
        if stock != twin:
            stock_mismatch.append(cell)
    print(f"cells: {len(CELLS)}")
    print(f"twin-vs-moto-merged mismatches: {len(merged_mismatch)}")
    for c in merged_mismatch:
        print(
            f"  MM {c['label']}: merged={c['merged']} twin={c['twin']} tag={c['tag']}"
        )
    print(f"twin-vs-moto-stock mismatches: {len(stock_mismatch)}")
    for c in stock_mismatch:
        print(
            f"  SM {c['label']}: stock={c['stock']} merged={c['merged']} twin={c['twin']} tag={c['tag']}"
        )
    untagged = [c for c in merged_mismatch if not c["tag"]]
    print(f"untagged merged mismatches (must be 0): {len(untagged)}")
    print(f"tagged-vs-merged agreement: { {k: v[1] for k, v in per_tag.items()} }")


if __name__ == "__main__":
    sys.exit(main())
