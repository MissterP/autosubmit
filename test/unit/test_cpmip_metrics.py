# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit. If not, see <http://www.gnu.org/licenses/>.

from types import SimpleNamespace

from autosubmit.metrics.cpmip_metrics import CPMIPMetrics


def _job(name="a000_SIM"):
    return SimpleNamespace(name=name)


def test_evaluate_greater_than_with_violation(mocker):
    thresholds = {
        "SYPD": {
            "THRESHOLD": 5.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        }
    }
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={"SYPD": 3.9},
    )

    violations = CPMIPMetrics.evaluate(_job(), thresholds)

    assert "SYPD" in violations
    assert violations["SYPD"]["threshold"] == 5.0
    assert violations["SYPD"]["accepted_error"] == 10
    assert violations["SYPD"]["real_value"] == 3.9


def test_evaluate_greater_than_boundary_is_not_violation(mocker):
    thresholds = {
        "SYPD": {
            "THRESHOLD": 5.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        }
    }
    # lower bound = 5.0 * (1 - 0.10) = 4.5
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={"SYPD": 4.5},
    )

    violations = CPMIPMetrics.evaluate(_job(), thresholds)

    assert violations == {}


def test_evaluate_less_than_with_violation(mocker):
    thresholds = {
        "LATENCY": {
            "THRESHOLD": 10.0,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 5,
        }
    }
    # upper bound = 10.0 * (1 + 0.05) = 10.5
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={"LATENCY": 10.6},
    )

    violations = CPMIPMetrics.evaluate(_job(), thresholds)

    assert "LATENCY" in violations
    assert violations["LATENCY"]["threshold"] == 10.0
    assert violations["LATENCY"]["accepted_error"] == 5
    assert violations["LATENCY"]["real_value"] == 10.6

def test_evaluate_less_than_boundary_is_not_violation(mocker):
    thresholds = {
        "LATENCY": {
            "THRESHOLD": 10.0,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 5,
        }
    }
    # upper bound = 10.0 * (1 + 0.05) = 10.5
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={"LATENCY": 10.5},
    )

    violations = CPMIPMetrics.evaluate(_job(), thresholds)

    assert violations == {}


def test_evaluate_mixed_metrics_only_returns_violations(mocker):
    thresholds = {
        "SYPD": {
            "THRESHOLD": 5.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        },
        "LATENCY": {
            "THRESHOLD": 10.0,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 5,
        },
    }
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={"SYPD": 3.9, "LATENCY": 10.4},
    )

    violations = CPMIPMetrics.evaluate(_job(), thresholds)

    assert list(violations.keys()) == ["SYPD"]


def test_evaluate_skips_missing_metric_in_fetched_values(mocker):
    thresholds = {
        "SYPD": {
            "THRESHOLD": 5.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        }
    }
    mocker.patch(
        "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
        return_value={},
    )

    violations = CPMIPMetrics.evaluate(_job(), thresholds)

    assert violations == {}