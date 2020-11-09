#!/usr/bin/env python2
"""
Script to run as subprocess in a gbasf2 environment (with ``run_with_gbasf``) to
interact with BelleDIRAC and if there is an alive DiracProxy in the system with
a positive amount of seconds left.
"""
import sys

from BelleDIRAC.gbasf2.lib.auth import getProxyInfo

if __name__ == "__main__":
    ProxyInfo = getProxyInfo()
    seconds = ProxyInfo["Value"]["secondsLeft"]
    if seconds > 0:
        sys.exit(0)
    sys.exit(1)
