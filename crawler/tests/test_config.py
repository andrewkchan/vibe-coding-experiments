import pytest
from pathlib import Path
import sys
import argparse

from crawler_module.config import parse_args, CrawlerConfig, DEFAULT_DATA_DIR, DEFAULT_MAX_WORKERS, DEFAULT_LOG_LEVEL

# Test basic parsing with required arguments
def test_parse_args_required(monkeypatch):
    test_seed_file = "seeds.txt"
    test_email = "test@example.com"
    
    # Create a dummy seed file for the test
    with open(test_seed_file, "w") as f:
        f.write("http://example.com\n")

    monkeypatch.setattr(sys, 'argv', [
        'script_name',
        '--seed-file', test_seed_file,
        '--email', test_email
    ])
    
    config = parse_args()
    
    assert config.seed_file == Path(test_seed_file)
    assert config.email == test_email
    assert config.data_dir == Path(DEFAULT_DATA_DIR)
    assert config.max_workers == DEFAULT_MAX_WORKERS
    assert config.log_level == DEFAULT_LOG_LEVEL.upper()
    assert config.user_agent == f"MyEducationalCrawler/1.0 (mailto:{test_email})"
    assert not config.resume
    assert config.exclude_file is None
    assert config.max_pages is None
    assert config.max_duration is None

    Path(test_seed_file).unlink() # Clean up dummy seed file

# Test parsing with all arguments
def test_parse_args_all(monkeypatch):
    test_seed_file = "all_seeds.txt"
    test_email = "all_args@example.com"
    test_data_dir = "./custom_data"
    test_exclude_file = "excludes.txt"
    test_max_workers = 100
    test_max_pages = 1000
    test_max_duration = 3600
    test_log_level = "DEBUG"

    with open(test_seed_file, "w") as f:
        f.write("http://example.com\n")
    with open(test_exclude_file, "w") as f:
        f.write("excluded.com\n")

    monkeypatch.setattr(sys, 'argv', [
        'script_name',
        '--seed-file', test_seed_file,
        '--email', test_email,
        '--data-dir', test_data_dir,
        '--exclude-file', test_exclude_file,
        '--max-workers', str(test_max_workers),
        '--max-pages', str(test_max_pages),
        '--max-duration', str(test_max_duration),
        '--log-level', test_log_level,
        '--resume'
    ])
    
    config = parse_args()
    
    assert config.seed_file == Path(test_seed_file)
    assert config.email == test_email
    assert config.data_dir == Path(test_data_dir)
    assert config.exclude_file == Path(test_exclude_file)
    assert config.max_workers == test_max_workers
    assert config.max_pages == test_max_pages
    assert config.max_duration == test_max_duration
    assert config.log_level == test_log_level.upper()
    assert config.resume is True
    assert config.user_agent == f"MyEducationalCrawler/1.0 (mailto:{test_email})"

    Path(test_seed_file).unlink()
    Path(test_exclude_file).unlink()

# Test for missing required arguments (argparse handles this by exiting)
def test_parse_args_missing_required(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['script_name', '--email', 'onlyemail@example.com'])
    with pytest.raises(SystemExit) as e:
        parse_args()
    assert e.value.code != 0 # SystemExit with non-zero code indicates error

    monkeypatch.setattr(sys, 'argv', ['script_name', '--seed-file', 'onlyseeds.txt'])
    with open("onlyseeds.txt", "w") as f: f.write("test\n") # argparse checks file existence if type=Path
    with pytest.raises(SystemExit) as e:
        parse_args()
    assert e.value.code != 0
    Path("onlyseeds.txt").unlink()

# Test invalid choice for log_level
def test_parse_args_invalid_log_level(monkeypatch):
    test_seed_file = "log_seeds.txt"
    with open(test_seed_file, "w") as f: f.write("test\n")
    monkeypatch.setattr(sys, 'argv', [
        'script_name',
        '--seed-file', test_seed_file,
        '--email', 'log@example.com',
        '--log-level', 'INVALID'
    ])
    with pytest.raises(SystemExit) as e:
        parse_args()
    assert e.value.code != 0
    Path(test_seed_file).unlink() 