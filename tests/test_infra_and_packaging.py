import pathlib
import re
import subprocess
import zipfile

from gtfsrt.lambda_alerts.tables import ACTIVE_TABLE_SPEC, EVENTS_TABLE_SPEC

ROOT = pathlib.Path(__file__).parent.parent
TF = (ROOT / "infra" / "terraform" / "alerts.tf").read_text()


def tf_resource(name: str) -> str:
    m = re.search(rf'resource "aws_dynamodb_table" "{name}" \{{(.*?)\n\}}\n', TF, re.S)
    assert m, f"terraform resource {name} not found"
    return m.group(1)


def test_terraform_dynamodb_tables_match_python_definitions():
    """Two copies of the schema exist (tables.py for tests/local, Terraform for AWS). They must agree."""
    for tf_name, spec in (("alert_events", EVENTS_TABLE_SPEC), ("active_alerts", ACTIVE_TABLE_SPEC)):
        body = tf_resource(tf_name)
        assert f'name         = "{spec["TableName"]}"' in body
        assert f'hash_key     = "{spec["KeySchema"][0]["AttributeName"]}"' in body
        for attr in spec["AttributeDefinitions"]:
            assert f'name = "{attr["AttributeName"]}"' in body and f'type = "{attr["AttributeType"]}"' in body
        assert 'attribute_name = "ttl"' in body  # TTL attribute used by the handler


def test_terraform_gsi_matches_python_definition():
    body = tf_resource("active_alerts")
    gsi = ACTIVE_TABLE_SPEC["GlobalSecondaryIndexes"][0]
    assert f'name            = "{gsi["IndexName"]}"' in body
    assert 'hash_key        = "status"' in body and 'range_key       = "event_time"' in body


def test_lambda_env_names_match_handler():
    # handler reads these env vars; Terraform must set exactly them
    handler = (ROOT / "src/gtfsrt/lambda_alerts/handler.py").read_text()
    for var in ("ALERT_TOPIC_ARN", "EVENTS_TABLE", "ACTIVE_TABLE"):
        assert f'"{var}"' in handler and f"{var} " in TF


def test_packages_contain_exactly_what_each_runtime_needs(tmp_path):
    subprocess.run(["bash", str(ROOT / "scripts/package_and_upload.sh"), "--build-only"], cwd=ROOT, check=True,
                   capture_output=True)
    lam = set(zipfile.ZipFile(ROOT / "dist/lambda_alerts.zip").namelist())
    glue = set(zipfile.ZipFile(ROOT / "dist/gtfsrt.zip").namelist())

    assert "gtfsrt/lambda_alerts/handler.py" in lam and "gtfsrt/lambda_alerts/tables.py" in lam
    assert not any("streaming" in n or "transform" in n or "warehouse" in n for n in lam), "lambda must stay minimal"

    assert "gtfsrt/transform/curate.py" in glue and "gtfsrt/runid.py" in glue
    assert not any("streaming" in n or "lambda_alerts" in n for n in glue)
    assert not any("__pycache__" in n or n.endswith(".pyc") for n in lam | glue)
    assert (ROOT / "dist/curate_job.py").exists()


def test_lambda_package_imports_cleanly_without_the_rest_of_the_repo(tmp_path):
    """Import the handler from ONLY the zip: catches an accidental import of something we did not ship."""
    subprocess.run(["bash", str(ROOT / "scripts/package_and_upload.sh"), "--build-only"], cwd=ROOT, check=True,
                   capture_output=True)
    code = "import sys; sys.path.insert(0, sys.argv[1]); import gtfsrt.lambda_alerts.handler as h; print(h.lambda_handler.__name__)"
    out = subprocess.run([str(ROOT / ".venv/bin/python"), "-I", "-c", code, str(ROOT / "dist/lambda_alerts.zip")],
                         cwd=tmp_path, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "lambda_handler"
