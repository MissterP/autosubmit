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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit. If not, see <http://www.gnu.org/licenses/>.

"""Integration tests for CPMIP checks using a real ``autosubmit run`` execution."""

from pathlib import Path
from types import SimpleNamespace
from typing import Generator, Tuple

import pytest
import requests
from ruamel.yaml import YAML
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.metrics.cpmip_metrics import CPMIPMetrics
from autosubmit.notifications.mail_notifier import _generate_message_cpmip_threshold_violations
from test.integration.test_mail import _normalize_mail_text
from test.integration.test_utils.networking import get_free_port


pytestmark = [pytest.mark.docker, pytest.mark.slurm, pytest.mark.ssh]


@pytest.fixture(scope="module")
def smtp_server() -> Generator[Tuple[str, str], None, None]:
    """Start SMTP test server and expose SMTP/API endpoints."""
    smtp_port = get_free_port()
    api_port = get_free_port()

    with DockerContainer(image="mailhog/mailhog", remove=True) \
            .with_bind_ports(1025, smtp_port) \
            .with_bind_ports(8025, api_port) as container:
        wait_for_logs(container, "Serving under")
        smtp_host_port = f"127.0.0.1:{smtp_port}"
        api_base = f"http://127.0.0.1:{api_port}"
        yield smtp_host_port, api_base

        requests.delete(f"{api_base}/api/v1/messages")


def _experiment_data_with_thresholds(thresholds, notifications="true"):
    fake_platforms = YAML().load(Path(__file__).resolve().parents[1] / "files/fake-platforms.yml")

    return {
        "DEFAULT": {
            "HPCARCH": "TEST_SLURM",
        },
        "MAIL": {
            "NOTIFICATIONS": notifications,
            "TO": ["user@example.com"],
        },
        "PLATFORMS": {
            "TEST_SLURM": fake_platforms["PLATFORMS"]["TEST_SLURM"],
        },
        "EXPERIMENT": {
            "DATELIST": "20200101",
            "MEMBERS": "fc0",
            "CHUNKSIZE": 1,
            "CHUNKSIZEUNIT": "year",
            "NUMCHUNKS": 1,
            "CALENDAR": "standard",
        },
        "JOBS": {
            "SIM": {
                "RUNNING": "chunk",
                "PLATFORM": "TEST_SLURM",
                "WALLCLOCK": "00:05",
                "PROCESSORS": 2,
                "SCRIPT": "echo CPMIP integration test; sleep 3",
                "CHUNKSIZE": 1,
                "CHUNKSIZEUNIT": "year",
                "CPMIP_THRESHOLDS": thresholds,
            }
        },
    }


def _read_sim_metrics_from_stat_file(exp, processors=2) -> tuple[float, float]:
    stat_file = next((exp.exp_path / "tmp").glob(f"{exp.expid}_*_SIM_STAT_0"), None)
    assert stat_file is not None, "SIM stat file was not recovered"

    lines = stat_file.read_text().strip().splitlines()
    assert len(lines) >= 2, f"Unexpected stat file format: {stat_file}"

    start_time = float(lines[0])
    finish_time = float(lines[1])
    runtime_hours = (finish_time - start_time) / 3600.0
    assert runtime_hours > 0, f"Non-positive runtime in stat file: {stat_file}"

    simulated_years = CPMIPMetrics.SY(1, "year")
    sypd = CPMIPMetrics.SYPD(runtime_hours, simulated_years)
    chsy = CPMIPMetrics.CHSY(runtime_hours, simulated_years, processors)
    return sypd, chsy


def _build_expected_cpmip_mail(expid: str, job_name: str, sypd: float, thresholds: dict, processors: int = 2) -> str:
    """Build expected CPMIP notification body from computed metrics and thresholds."""
    runtime_hours = 24.0 / sypd
    start_time = 0
    finish_time = runtime_hours * 3600.0

    evaluation_job = SimpleNamespace(
        start_time_timestamp=start_time,
        finish_time_timestamp=finish_time,
        chunk_size=1,
        chunk_size_unit="year",
        total_processors=processors,
    )
    violations = CPMIPMetrics.evaluate(evaluation_job, thresholds)

    return _generate_message_cpmip_threshold_violations(expid, job_name, violations)


def test_cpmip_violation_notification_is_emitted_after_full_run(autosubmit_exp, monkeypatch, smtp_server,
                                                                 slurm_server):
    smtp_host_port, api_base = smtp_server
    requests.delete(f"{api_base}/api/v1/messages")

    monkeypatch.setattr(BasicConfig, "MAIL_FROM", "notifier@localhost")
    monkeypatch.setattr(BasicConfig, "SMTP_SERVER", smtp_host_port)

    thresholds = {
        "SYPD": {
            "THRESHOLD": 0.01,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 0,
        },
        "CHSY": {
            "THRESHOLD": 0.0001,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 0,
        },
    }
    exp = autosubmit_exp(experiment_data=_experiment_data_with_thresholds(thresholds))
    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, "run")

    exit_code = exp.autosubmit.run_experiment(exp.expid)

    assert exit_code == 0

    sypd, chsy = _read_sim_metrics_from_stat_file(exp)
    assert sypd > 0.01, f"Expected SYPD > 0.01 for less_than violation, got {sypd}"
    assert chsy > 0.0001, f"Expected CHSY > 0.0001 for less_than violation, got {chsy}"

    stat_file = next((exp.exp_path / "tmp").glob(f"{exp.expid}_*_SIM_STAT_0"), None)
    job_name = stat_file.name.replace("_STAT_0", "")

    response = requests.get(f"{api_base}/api/v2/messages")
    assert response.json()["count"] == 1

    email = response.json()["items"][0]
    assert "CPMIP Threshold Violation detected" in email["Content"]["Headers"]["Subject"][0]

    expected_mail = _build_expected_cpmip_mail(exp.expid, job_name, sypd, thresholds, processors=2)
    assert _normalize_mail_text(email["Content"]["Body"]) == _normalize_mail_text(expected_mail)
    assert "notifier@localhost" in email["Raw"]["From"]
    assert any("user@example.com" in recipient for recipient in email["Raw"]["To"])


def test_cpmip_notification_is_not_emitted_when_threshold_not_violated_after_full_run(autosubmit_exp, monkeypatch,
                                                                                       smtp_server,
                                                                                       slurm_server):
    smtp_host_port, api_base = smtp_server
    requests.delete(f"{api_base}/api/v1/messages")

    monkeypatch.setattr(BasicConfig, "MAIL_FROM", "notifier@localhost")
    monkeypatch.setattr(BasicConfig, "SMTP_SERVER", smtp_host_port)

    exp = autosubmit_exp(
        experiment_data=_experiment_data_with_thresholds(
            {
                "SYPD": {
                    "THRESHOLD": 1.0,
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": 0,
                }
            }
        )
    )
    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, "run")

    exit_code = exp.autosubmit.run_experiment(exp.expid)

    assert exit_code == 0
    sypd, chsy = _read_sim_metrics_from_stat_file(exp)
    assert sypd > 1.0, f"Expected SYPD > 1.0 for greater_than non-violation, got {sypd}"
    assert chsy > 0

    response = requests.get(f"{api_base}/api/v2/messages")
    assert response.json()["count"] == 0


def test_cpmip_violation_notification_is_not_emitted_when_notifications_are_disabled(autosubmit_exp, monkeypatch,
                                                                                      smtp_server,
                                                                                      slurm_server):
    smtp_host_port, api_base = smtp_server
    requests.delete(f"{api_base}/api/v1/messages")

    monkeypatch.setattr(BasicConfig, "MAIL_FROM", "notifier@localhost")
    monkeypatch.setattr(BasicConfig, "SMTP_SERVER", smtp_host_port)

    exp = autosubmit_exp(
        experiment_data=_experiment_data_with_thresholds(
            {
                "SYPD": {
                    "THRESHOLD": 0.01,
                    "COMPARISON": "less_than",
                    "%_ACCEPTED_ERROR": 0,
                }
            },
            notifications="false"
        )
    )
    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, "run")

    exit_code = exp.autosubmit.run_experiment(exp.expid)

    assert exit_code == 0
    response = requests.get(f"{api_base}/api/v2/messages")
    assert response.json()["count"] == 0


def test_cpmip_notification_is_not_emitted_when_not_violated_and_notifications_are_disabled(
        autosubmit_exp, monkeypatch, smtp_server, slurm_server):
    smtp_host_port, api_base = smtp_server
    requests.delete(f"{api_base}/api/v1/messages")

    monkeypatch.setattr(BasicConfig, "MAIL_FROM", "notifier@localhost")
    monkeypatch.setattr(BasicConfig, "SMTP_SERVER", smtp_host_port)

    exp = autosubmit_exp(
        experiment_data=_experiment_data_with_thresholds(
            {
                "SYPD": {
                    "THRESHOLD": 1.0,
                    "COMPARISON": "greater_than",
                    "%_ACCEPTED_ERROR": 0,
                }
            },
            notifications="false"
        )
    )
    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, "run")

    exit_code = exp.autosubmit.run_experiment(exp.expid)

    assert exit_code == 0
    response = requests.get(f"{api_base}/api/v2/messages")
    assert response.json()["count"] == 0
