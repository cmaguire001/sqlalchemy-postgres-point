# `infer_axis_order()` — Formal Guarantee Document

## What this function does

Given a batch of `(a, b)` coordinate pairs from an unknown source, it returns
a calibrated probability that `a` is longitude and `b` is latitude — along with
a full audit trail of every signal that contributed to that probability.

---

## What this function guarantees

**G1 — It never mutates data.**
It only recommends. The caller decides whether to reorder. Always.

**G2 — It never returns false certainty.**
If signals contradict each other, `recommendation` is `"do_not_reorder"`
regardless of the probability score.

**G3 — It is honest about its own limits.**
Every signal reports whether its preconditions were met. If a signal couldn't
run cleanly, it abstains. Abstentions are visible in the output.

**G4 — Uncertain is a valid and complete answer.**
`"recommended_order": "uncertain"` is not a failure. It is the correct answer
when the data does not contain enough information to decide.

**G5 — It is auditable.**
Every recommendation can be traced back to specific signal votes and weights.
No black box scores.

---

## What this function does not guarantee

**N1 — It cannot resolve a single ambiguous point.**
Inference requires a population. Minimum reliable sample size is 30 points.
Below that, signals are statistically unreliable and the function says so.

**N2 — It cannot distinguish two mixed-source batches from genuine uncertainty.**
If you concatenate a correctly-ordered dataset with a swapped one, the function
will detect contradiction but cannot tell you which half is wrong.

**N3 — It cannot infer axis order for data symmetric about both axes simultaneously.**
This is a hard information-theoretic limit. No algorithm can exceed 50% confidence
on a batch whose `(a,b)` joint distribution is identical to its `(b,a)` distribution.

**N4 — It is not a substitute for knowing your data source.**
If you know the data came from a specific region or instrument, use that knowledge.
`region_hint` exists for exactly this reason. Inference is a last resort.

---

## Domain-specific reliability

| Domain | Reliable? | Limiting factor |
|---|---|---|
| Ski resorts, cities, fixed infrastructure | Yes | Strong hemisphere coherence |
| Long-haul vehicle or vessel routes | Yes with timestamps | Trajectory coherence required |
| Open ocean, wide area maritime | Partial | Spread signals weaken over large areas |
| Polar regions (lat > 75°) | No | All spread signals degrade near poles |
| Cross-antimeridian trajectories | Yes if detected | Antimeridian unwrapping applied automatically |
| Single point | Never | Mathematically unresolvable without context |
| Mixed-source batch | Flagged only | Can detect contradiction, cannot resolve it |

---

## Failure mode classification

| Type | Description | Severity | Library behavior |
|---|---|---|---|
| I | False swap correction — valid data reordered incorrectly | Critical | Prevented by G1 — library never reorders |
| II | Missed swap — swapped data passes as valid | High | Mitigated by conservative threshold (0.85) |
| III | False certainty — wrong answer with high confidence | Critical | Prevented by G2 — contradiction blocks recommendation |
| IV | Silent mixed-source — two datasets treated as one | High | Mitigated by bimodality detection in `data_quality` |

---

## The one rule

> When in doubt, do not reorder.

A pipeline that passes ambiguous data upstream for human review is safer than
one that silently corrects it and is wrong.

---

*This document is the contract. The implementation is tested against it.*
*If the code and this document disagree, the document wins.*
