#!/usr/bin/env python3
"""Script to refurn Lustre quota as a JSON object for ingesting in to XDMoD"""

import subprocess
import datetime
import json


def storage(username, filesystem="/lustre"):
    """Returns a dict"""
    lfs_quota = subprocess.run(
        [
            "lfs",
            "quota",
            "-q",
            "-u",
            username,
            filesystem,
        ],
        check=True,
        stdout=subprocess.PIPE,
    )
    fields = lfs_quota.stdout.decode("utf-8")
    [
        filesystem,
        kbytes,
        quota,
        limit,
        _,
        files,
        _,
        _,
        _,
    ] = fields.split()
    iso_dt = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    usage = {
        "resource": "Lustre",
        "mountpoint": filesystem,
        "user": username,
        "pi": username,
        "dt": iso_dt,
        "soft_threshold": int(quota) * 1024,
        "hard_threshold": int(limit) * 1024,
        "file_count": int(files.strip('*')),
        "logical_usage": int(kbytes.strip('*')) * 1024,
        "physical_usage": int(kbytes.strip('*')) * 1024,
    }
    return usage


def all_users():
    """Returns a list of all the usernames"""

    cmd = subprocess.run(
        ["ipa", "user-find", "--sizelimit=0"],
        check=True,
        stdout=subprocess.PIPE,
    )
    usernames = []
    for line in cmd.stdout.decode("utf-8").split("\n"):
        if "User login: " in line:
            usernames.append(line.split()[2])
    return usernames


if __name__ == "__main__":
    metrics = []
    for user in all_users():
        metrics.append(storage(user))
    print(json.dumps(metrics, indent=2))
