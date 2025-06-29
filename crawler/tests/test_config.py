import sys
from pathlib import Path
import pytest
from crawler_module.config import CrawlerConfig, DEFAULT_DATA_DIR, DEFAULT_MAX_WORKERS, DEFAULT_LOG_LEVEL

def test_config_from_args_required(monkeypatch):
    """Test that CrawlerConfig can be created with only the required arguments."""
    test_seed_file = "seeds.txt"
    test_email = "test@example.com"
    
    # Create a dummy seed file
    with open(test_seed_file, "w") as f:
        f.write("http://example.com\n")

    monkeypatch.setattr(sys, 'argv', [
        'script_name',
        '--seed-file', test_seed_file,
        '--email', test_email
    ])

    config = CrawlerConfig.from_args()

    assert config.seed_file == Path(test_seed_file)
    assert config.email == test_email
    assert config.data_dir == Path(DEFAULT_DATA_DIR)
    assert config.max_workers == DEFAULT_MAX_WORKERS
    assert config.log_level == DEFAULT_LOG_LEVEL.upper()
    assert config.user_agent == f"MyEducationalCrawler/1.0 (mailto:{test_email})"
    
    Path(test_seed_file).unlink()

def test_config_from_args_all(monkeypatch):
    """Test creating CrawlerConfig with all possible arguments."""
    test_seed_file = "all_seeds.txt"
    test_email = "all_args@example.com"
    test_data_dir = "./custom_data"
    test_exclude_file = "excludes.txt"
    test_max_workers = "100"
    test_max_pages = "1000"
    test_max_duration = "3600"
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
        '--max-workers', test_max_workers,
        '--max-pages', test_max_pages,
        '--max-duration', test_max_duration,
        '--log-level', test_log_level,
        '--resume'
    ])

    config = CrawlerConfig.from_args()

    assert config.seed_file == Path(test_seed_file)
    assert config.email == test_email
    assert config.data_dir == Path(test_data_dir)
    assert config.exclude_file == Path(test_exclude_file)
    assert config.max_workers == int(test_max_workers)
    assert config.max_pages == int(test_max_pages)
    assert config.max_duration == int(test_max_duration)
    assert config.log_level == test_log_level.upper()
    assert config.resume is True
    assert config.user_agent == f"MyEducationalCrawler/1.0 (mailto:{test_email})"

    Path(test_seed_file).unlink()
    Path(test_exclude_file).unlink()

def test_required_args_missing(monkeypatch):
    """Test that the script exits if required arguments are missing."""
    # Test without --seed-file
    monkeypatch.setattr(sys, 'argv', ['script_name', '--email', 'onlyemail@example.com'])
    with pytest.raises(SystemExit) as e:
        CrawlerConfig.from_args()
    assert e.value.code != 0

    # Test without --email
    monkeypatch.setattr(sys, 'argv', ['script_name', '--seed-file', 'onlyseeds.txt'])
    with open("onlyseeds.txt", "w") as f:
        f.write("test\n")
    with pytest.raises(SystemExit) as e:
        CrawlerConfig.from_args()
    assert e.value.code != 0
    Path("onlyseeds.txt").unlink()

def test_db_type_validation(monkeypatch):
    """Test that an invalid db-type causes an exit."""
    test_seed_file = "validation_seeds.txt"
    test_email = "validation@example.com"
    with open(test_seed_file, "w") as f:
        f.write("test\n")

    monkeypatch.setattr(sys, 'argv', [
        'script_name', '--seed-file', test_seed_file, '--email', test_email,
        '--db-type', 'invalid_db'
    ])
    with pytest.raises(SystemExit) as e:
        CrawlerConfig.from_args()
    assert e.value.code != 0
    Path(test_seed_file).unlink() 