import datetime
import json
import logging.config
import operator
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Optional, Iterable, Literal

import boto3
from boto3 import Session
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import ListObjectsV2OutputTypeDef

_PATH_CONFIG: Path = Path('.') / 'config.json'
_APP_LOGGER_ROOT_NAME: str = "backuper_to_s3"

logging.config.dictConfig({
	'version': 1,
	'formatters': {
		'complete_formatter': {
			'format': '[$levelname]\t[$asctime]\t[$pathname]\t$message',
			'style': '$',
			'datefmt': '%Y-%m-%d %H:%M:%S%z'
		},
		'simple_formatter': {

		}
	},
	'handlers': {
		'console': {
			'class': 'logging.StreamHandler',
			'level': 'DEBUG',
			'formatter': 'complete_formatter',
			'stream': 'ext://sys.stdout'
		}
	},
	'loggers': {
		_APP_LOGGER_ROOT_NAME: {
			'handlers': ['console'],
			'level': 'DEBUG',
		}
	},
	'root': {
		'level': 'DEBUG'
	},
	'disable_existing_loggers': False,
})
logger = logging.getLogger(".".join([_APP_LOGGER_ROOT_NAME, __name__]))


@dataclass
class Config:
	aws_access_key_id: str
	aws_secret_access_key: str
	region_name: str
	backup_dir_key_prefix: str
	bucket: str
	path_local_backups: str
	backup_ttl_seconds: int
	remote_timestamp_format: str
	local_timestamp_format: str

	@classmethod
	def from_config(cls) -> 'Config':
		with open(_PATH_CONFIG, encoding='utf8', mode='r') as f:
			return cls(**json.load(f))


@dataclass
class TimestampedLocalBackup:
	timestamp: datetime.datetime
	path: Path

	@classmethod
	def maybe_parse(cls, cfg: Config, path: Path) -> Optional['TimestampedLocalBackup']:
		if (timestamp := path_to_datetime(cfg, 'local', path)) is not None:
			return cls(timestamp=timestamp, path=path)
		return None

	def to_remote_key(self, cfg: Config) -> str:
		formatted_timestamp: str = self.timestamp.strftime(cfg.remote_timestamp_format)

		return f"{cfg.backup_dir_key_prefix}{formatted_timestamp}.zip"


def session_from_config(cfg: Config) -> boto3.session.Session:
	return boto3.session.Session(
		aws_access_key_id=cfg.aws_access_key_id,
		aws_secret_access_key=cfg.aws_secret_access_key,
		region_name=cfg.region_name
	)


def backups_in_s3(client: S3Client, cfg: Config) -> set[datetime.datetime]:
	response: ListObjectsV2OutputTypeDef = client.list_objects_v2(Bucket=cfg.bucket, Prefix=cfg.backup_dir_key_prefix)
	stored_keys: Iterable[Path] = (Path(o["Key"]) for o in response["Contents"])

	return set(filter(
		partial(operator.is_not, None),
		map(partial(path_to_datetime, cfg, 'remote'), stored_keys)
	))


def backups_local(cfg: Config) -> list[TimestampedLocalBackup]:
	backups_path: Path = Path(cfg.path_local_backups)
	local_paths: Iterable[Path] = (p for p in backups_path.iterdir() if p.is_file() and p.suffix.lower() == '.zip')
	return list(filter(
		partial(operator.is_not, None),
		map(partial(TimestampedLocalBackup.maybe_parse, cfg), local_paths)
	))


def path_to_datetime(cfg: Config, timestamp_source: Literal['remote', 'local'], path: Path) -> datetime.datetime | None:
	timestamp_format: str
	match timestamp_source:
		case 'remote':
			timestamp_format = cfg.remote_timestamp_format
		case 'local':
			timestamp_format = cfg.local_timestamp_format
		case source:
			raise ValueError(f"Unknown timestamp source: {source}")

	raw_datetime: str = path.with_suffix('').name
	try:
		return datetime.datetime.strptime(raw_datetime, timestamp_format).replace(tzinfo=datetime.UTC)
	except ValueError:
		return None


def expired(cfg: Config, current_timestamp: datetime.datetime, local_backup: TimestampedLocalBackup, ) -> bool:
	return (current_timestamp - local_backup.timestamp).total_seconds() > cfg.backup_ttl_seconds


def upload_backup(cfg: Config, s3_client: S3Client, local_backup: TimestampedLocalBackup) -> None:
	key: str = local_backup.to_remote_key(cfg)
	logger.info(f"Uploading backup file {local_backup} to S3 bucket {cfg.bucket} under {key}")
	s3_client.upload_file(Filename=str(local_backup.path), Bucket=cfg.bucket, Key=key)


def main() -> None:
	logger.info("Started")
	cfg: Config = Config.from_config()
	session: Session = session_from_config(cfg)
	s3_client: S3Client = session.client('s3')
	logger.debug("Connected")

	remote_backups: set[datetime.datetime] = backups_in_s3(s3_client, cfg)
	logger.debug(f"Found {len(remote_backups)} remote backups: {remote_backups}")
	local_backups: list[TimestampedLocalBackup] = backups_local(cfg)
	logger.debug(f"Found {len(local_backups)} local backups: {local_backups}")

	timestamp_now: datetime.datetime = datetime.datetime.now(datetime.UTC)

	must_upload_backups: list[TimestampedLocalBackup] = list(filter(
		lambda local: local.timestamp not in remote_backups and not expired(cfg, timestamp_now, local),
		local_backups
	))

	logger.debug(f"Files to be uploaded ({len(must_upload_backups)}): {must_upload_backups}")

	for must_upload_backup in must_upload_backups:
		upload_backup(cfg, s3_client, must_upload_backup)


if __name__ == '__main__':
	try:
		main()
	except Exception as e:
		logger.exception(f"Exception occurred: {e!r}")
