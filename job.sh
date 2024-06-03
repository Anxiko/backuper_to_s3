#!/bin/bash
cd "$(dirname "$0")" || exit;
python backup.py >> backuper_to_s3.log 2>&1