# Peerster-Tests
Testing Framework for the DSE Peerster application

## Get started
1. Change PEERSTER_ROOT in "tests.py"
2. Install pytest
2. Run `pytest -q tests.py`

## Troubleshooting
* Increasing timeouts might help if tests are failing
* When test fails, peersters are sometimes not closed correctly (TODO: improve this), run `pkill Peerster`
