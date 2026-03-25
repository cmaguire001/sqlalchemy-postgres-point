"""

- Points should be stored as (longitude, latitude)

References:

- https://stackoverflow.com/questions/37233116/point-type-in-sqlalchemy
- https://gist.github.com/kwatch/02b1a5a8899b67df2623
- https://geoalchemy.readthedocs.io/en/0.5/intro.html
"""

import logging
import math
import re
import warnings
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Optional, Tuple

from sqlalchemy.types import Float, UserDefinedType


@dataclass(frozen=True)
class PointValidationResult:
    valid: bool
    swap_detected: bool
    confidence: float
    ambiguous: bool
    normalized: Optional[Tuple[float, float]]
    issues: Tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly representation."""
        return asdict(self)


def analyze_point(value: Any) -> PointValidationResult:
    """Analyze a raw point-like value and return validation metadata.

    Confidence is a heuristic score in [0, 1] indicating how confident we are
    that coordinates are in intended (lng, lat) order.
    """
    issues: list[str] = []
    swap_detected = False
    ambiguous = False

    try:
        lng, lat = value  # type: ignore[misc]
    except Exception:
        return PointValidationResult(
            valid=False,
            swap_detected=False,
            confidence=0.0,
            ambiguous=False,
            normalized=None,
            issues=("Point must be a 2-tuple of (lng, lat)",),
        )

    try:
        lng_f = float(lng)
        lat_f = float(lat)
    except Exception:
        return PointValidationResult(
            valid=False,
            swap_detected=False,
            confidence=0.0,
            ambiguous=False,
            normalized=None,
            issues=("Point coordinates must be numeric",),
        )

    if not (math.isfinite(lng_f) and math.isfinite(lat_f)):
        return PointValidationResult(
            valid=False,
            swap_detected=False,
            confidence=0.0,
            ambiguous=False,
            normalized=None,
            issues=("Point coordinates must be finite numbers",),
        )

    lng_in_range = -180.0 <= lng_f <= 180.0
    lat_in_range = -90.0 <= lat_f <= 90.0

    if not lng_in_range:
        issues.append("Longitude must be within [-180, 180]")
    if not lat_in_range:
        issues.append("Latitude must be within [-90, 90]")

    # Likely swap: first value looks like latitude, second value only fits longitude.
    if -90.0 <= lng_f <= 90.0 and 90.0 < abs(lat_f) <= 180.0:
        swap_detected = True
        issues.append("Coordinates are likely swapped (received lat,lng)")

    # Ambiguous but valid: both values are plausible latitudes.
    if -90.0 <= lng_f <= 90.0 and -90.0 <= lat_f <= 90.0 and lng_in_range and lat_in_range:
        ambiguous = True

    valid = lng_in_range and lat_in_range

    # Confidence heuristics (distance from equator and prime meridian + ambiguity checks).
    if not valid:
        confidence = 0.0
    elif swap_detected:
        confidence = 0.05
    else:
        # Slight preference when longitude shows stronger spread than latitude.
        lat_signal = abs(lat_f) / 90.0
        lng_signal = abs(lng_f) / 180.0
        role_signal = 0.5 + (lng_signal - lat_signal) * 0.35

        # Tiny boost for points farther from 0,0 where axis order is less obvious.
        origin_signal = min(0.2, (abs(lat_f) + abs(lng_f)) / 900.0)

        confidence = max(0.0, min(1.0, role_signal + origin_signal))
        if ambiguous:
            confidence = min(confidence, 0.62)

    return PointValidationResult(
        valid=valid,
        swap_detected=swap_detected,
        confidence=round(confidence, 4),
        ambiguous=ambiguous,
        normalized=(lng_f, lat_f) if valid else None,
        issues=tuple(issues),
    )


def validate_points(values: Iterable[Any]) -> dict[str, Any]:
    """Batch-validate point-like values and return summary metrics.

    The output is JSON-friendly and can be sent directly to logging/ETL layers.
    """
    rows = list(values)
    flagged_rows: list[dict[str, Any]] = []
    swap_count = 0
    invalid_count = 0
    ambiguous_count = 0

    for idx, point in enumerate(rows):
        analysis = analyze_point(point)
        if analysis.swap_detected:
            swap_count += 1
        if analysis.ambiguous:
            ambiguous_count += 1
        if not analysis.valid and not analysis.swap_detected:
            invalid_count += 1
        if (not analysis.valid) or analysis.swap_detected or analysis.ambiguous:
            flagged_rows.append(
                {
                    "index": idx,
                    "value": point,
                    "analysis": analysis.as_dict(),
                }
            )

    total = len(rows)
    if total == 0:
        return {
            "total_rows": 0,
            "likely_swapped_pct": 0.0,
            "invalid_pct": 0.0,
            "ambiguous_pct": 0.0,
            "flagged_rows": [],
        }

    return {
        "total_rows": total,
        "likely_swapped_pct": round((swap_count / total) * 100.0, 2),
        "invalid_pct": round((invalid_count / total) * 100.0, 2),
        "ambiguous_pct": round((ambiguous_count / total) * 100.0, 2),
        "flagged_rows": flagged_rows,
    }


class PointType(UserDefinedType):
    cache_ok = True

    def __init__(
        self,
        strict: bool = False,
        strict_mode: str = "warn",
        logger: Optional[logging.Logger] = None,
    ):
        if strict_mode not in {"warn", "error"}:
            raise ValueError("strict_mode must be either 'warn' or 'error'")
        self.strict = strict
        self.strict_mode = strict_mode
        self.logger = logger

    def get_col_spec(self, **kw):
        return "POINT"

    def _emit_strict_signal(self, message: str):
        if self.strict_mode == "error":
            raise ValueError(message)
        if self.logger is not None:
            self.logger.warning(message)
        else:
            warnings.warn(message, UserWarning, stacklevel=3)

    def _validate_point(self, value: Tuple[float, float]) -> Tuple[float, float]:
        """Validate and normalize a (lng, lat) tuple.

        - Ensures it's a 2-length tuple/list
        - Casts to float and checks finiteness (no NaN/inf)
        - Checks ranges: lng in [-180, 180], lat in [-90, 90]
        - In strict mode, flags ambiguous (lng, lat) values where both axes
          could plausibly be latitude.
        """
        analysis = analyze_point(value)

        if not analysis.valid:
            if analysis.issues:
                raise ValueError(analysis.issues[0])
            raise ValueError("Invalid POINT value")

        if self.strict and analysis.ambiguous:
            self._emit_strict_signal(
                "Ambiguous POINT coordinates detected; both values are in latitude range "
                "[-90, 90], so (lng, lat) order cannot be validated confidently"
            )

        assert analysis.normalized is not None
        return analysis.normalized

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            lng, lat = self._validate_point(value)
            return f"({lng},{lat})"

        return process

    def literal_processor(self, dialect):
        def process(value):
            if value is None:
                return "NULL"
            lng, lat = self._validate_point(value)
            return f"'({lng},{lat})'"

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            match = re.match(r"^\((-?[\d.]+(?:[eE][+-]?\d+)?),(-?[\d.]+(?:[eE][+-]?\d+)?)\)$", value)
            if match:
                lng = float(match.group(1))
                lat = float(match.group(2))
                # Validate loaded values as well to preserve invariants in Python domain
                lng, lat = self._validate_point((lng, lat))
                return (lng, lat)
            raise ValueError(f"Invalid POINT value: {value}")

        return process

    class comparator_factory(UserDefinedType.Comparator):
        def earth_distance(self, other):
            """Compute earth distance using the <@> operator, returning a Float."""
            return self.op("<@>", return_type=Float())(other)
