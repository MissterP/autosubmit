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

import pytest

from autosubmit.metrics.cpmip_metrics import CPMIPMetrics


def _job(name="a000_SIM"):
    return SimpleNamespace(name=name)


class TestEvaluateCPMIPMetrics:

    def test_evaluate_greater_than_with_violation(self, mocker):
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
        assert violations["SYPD"]["comparison"] == "greater_than"
        assert violations["SYPD"]["bound"] == pytest.approx(4.5)
        assert violations["SYPD"]["real_value"] == 3.9

    def test_evaluate_less_than_with_violation(self, mocker):
        thresholds = {
            "LATENCY": {
                "THRESHOLD": 10.0,
                "COMPARISON": "less_than",
                "%_ACCEPTED_ERROR": 5,
            }
        }
        mocker.patch(
            "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
            return_value={"LATENCY": 10.6},
        )

        violations = CPMIPMetrics.evaluate(_job(), thresholds)

        assert "LATENCY" in violations
        assert violations["LATENCY"]["threshold"] == 10.0
        assert violations["LATENCY"]["accepted_error"] == 5
        assert violations["LATENCY"]["comparison"] == "less_than"
        assert violations["LATENCY"]["bound"] == pytest.approx(10.5)
        assert violations["LATENCY"]["real_value"] == 10.6

    def test_evaluate_boundary_is_not_violation_for_greater_than(self, mocker):
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

    def test_evaluate_boundary_is_not_violation_for_less_than(self, mocker):
        thresholds = {
            "LATENCY": {
                "THRESHOLD": 10.0,
                "COMPARISON": "less_than",
                "%_ACCEPTED_ERROR": 10,
            }
        }
        # upper bound = 10.0 * (1 + 0.10) = 11.0
        mocker.patch(
            "autosubmit.metrics.cpmip_metrics.CPMIPMetrics._fetch_metrics",
            return_value={"LATENCY": 11.0},
        )

        violations = CPMIPMetrics.evaluate(_job(), thresholds)

        assert violations == {}

    def test_evaluate_mixed_metrics_only_returns_violations(self, mocker):
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

    def test_evaluate_returns_empty_when_metrics_fetch_returns_empty(self, mocker):
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


class TestFetchCPMIPMetrics:

    def test_fetch_metrics_calls_sy_and_sypd_and_returns_sypd(self, mocker):
        job = SimpleNamespace(
            runtime=12.0,
            chunk_size=2,
            chunk_size_unit="month",
        )

        sy_mock = mocker.patch(
            "autosubmit.metrics.cpmip_metrics.CPMIPMetrics.SY",
            return_value=0.5,
        )
        sypd_mock = mocker.patch(
            "autosubmit.metrics.cpmip_metrics.CPMIPMetrics.SYPD",
            return_value=1.0,
        )

        metrics = CPMIPMetrics._fetch_metrics(job)

        sy_mock.assert_called_once_with(2, "month")
        sypd_mock.assert_called_once_with(12.0, 0.5)
        assert metrics == {"SYPD": 1.0}

    def test_fetch_metrics_returns_empty_when_chunk_unit_is_unsupported(self):
        job = SimpleNamespace(
            runtime=12,
            chunk_size=1,
            chunk_size_unit="week",
        )

        metrics = CPMIPMetrics._fetch_metrics(job)

        assert metrics == {}

    def test_fetch_metrics_returns_empty_when_runtime_is_non_positive(self):
        job = SimpleNamespace(
            runtime=0,
            chunk_size=1,
            chunk_size_unit="year",
        )

        metrics = CPMIPMetrics._fetch_metrics(job)

        assert metrics == {}

    def test_fetch_metrics_returns_empty_when_chunk_unit_is_non_positive(self):
        job = SimpleNamespace(
            runtime=12,
            chunk_size=-1,
            chunk_size_unit="year",
        )

        metrics = CPMIPMetrics._fetch_metrics(job)

        assert metrics == {}

    def test_fetch_metrics_returns_empty_when_chunk_metadata_uses_legacy_aliases_only(self):
        job = SimpleNamespace(
            runtime=24.0,
            chunksize=365,
            chunksizeunit="day",
        )

        metrics = CPMIPMetrics._fetch_metrics(job)

        assert metrics == {}


class TestComputationCPMIPMetrics:

    def test_sy_converts_chunk_to_simulated_years(self):
        assert CPMIPMetrics.SY(1, "year") == pytest.approx(1.0)
        assert CPMIPMetrics.SY(12, "month") == pytest.approx(1.0)
        assert CPMIPMetrics.SY(365, "day") == pytest.approx(1.0)
        assert CPMIPMetrics.SY(8760, "hour") == pytest.approx(1.0)

    def test_sypd_formula_uses_runtime_in_hours(self):
        # 1 simulated year in 12h runtime -> 2 simulated years per day
        assert CPMIPMetrics.SYPD(12, 1.0) == pytest.approx(2.0)

    def test_sy_raises_value_error_for_unsupported_chunk_size_unit(self):
        with pytest.raises(ValueError, match="Unsupported chunk_size_unit"):
            CPMIPMetrics.SY(1, "week")
    
    def test_sy_raises_value_error_for_non_positive_chunk_size(self):
        with pytest.raises(ValueError, match="chunk_size must be > 0"):
            CPMIPMetrics.SY(0, "year")

    @pytest.mark.parametrize("runtime", [0, -3])
    def test_sypd_raises_value_error_for_non_positive_runtime(self, runtime):
        with pytest.raises(ValueError, match="runtime must be > 0 hours"):
            CPMIPMetrics.SYPD(runtime, 1.0)

    def test_sypd_raises_value_error_for_non_positive_simulated_years(self):
        with pytest.raises(ValueError, match="simulated_years must be > 0"):
            CPMIPMetrics.SYPD(12, 0)