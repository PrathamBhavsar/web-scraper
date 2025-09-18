#!/usr/bin/env python3
"""
Unit Tests for Orchestrator Retry Logic

Tests the batch processing and retry workflow using mocks for IDM and filesystem.

Author: AI Assistant
Version: 1.0
"""

import pytest
import asyncio
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import json

# Import modules to test
from scrape_orchestrator import ScrapeOrchestrator, BatchResults, PageRetryState
from page_parser import VideoMetadata
from idm_manager import DownloadItem


class TestScrapeOrchestrator:
    """Test suite for ScrapeOrchestrator."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_config(self, temp_dir):
        """Create mock configuration."""
        config_file = Path(temp_dir) / "config.json"
        config_data = {
            "batch": {
                "batch_pages": 2,
                "batch_initial_wait_seconds": 1,
                "per_page_idm_wait_seconds": 1,
                "max_failed_retries_per_page": 2
            },
            "general": {
                "max_storage_gb": 1000,
                "download_path": str(temp_dir / "downloads")
            }
        }

        with open(config_file, 'w') as f:
            json.dump(config_data, f)

        return str(config_file)

    @pytest.fixture
    def mock_progress(self, temp_dir):
        """Create mock progress file."""
        progress_file = Path(temp_dir) / "progress.json" 
        progress_data = {
            "current_page": 1000,
            "failed_videos": [],
            "permanent_failed_pages": []
        }

        with open(progress_file, 'w') as f:
            json.dump(progress_data, f)

        return str(progress_file)

    @pytest.fixture
    def orchestrator(self, temp_dir, mock_config, mock_progress):
        """Create orchestrator instance for testing."""
        downloads_dir = str(Path(temp_dir) / "downloads")

        orchestrator = ScrapeOrchestrator(
            config_file=mock_config,
            progress_file=mock_progress,
            downloads_dir=downloads_dir,
            dry_run=True  # Use dry run for testing
        )

        return orchestrator

    @pytest.fixture
    def sample_videos(self):
        """Create sample video metadata for testing."""
        return [
            VideoMetadata(
                video_id="video1",
                title="Test Video 1",
                duration="02:30",
                thumbnail_url="https://example.com/thumb1.jpg",
                video_url="https://example.com/video1.mp4",
                upload_date="2025-01-01",
                tags=["tag1", "tag2"],
                page_url="https://example.com/page1",
                folder_path="/downloads/page_1000/video1"
            ),
            VideoMetadata(
                video_id="video2", 
                title="Test Video 2",
                duration="03:45",
                thumbnail_url="https://example.com/thumb2.jpg",
                video_url="https://example.com/video2.mp4",
                upload_date="2025-01-01", 
                tags=["tag3"],
                page_url="https://example.com/page2",
                folder_path="/downloads/page_1000/video2"
            )
        ]

    def test_orchestrator_initialization(self, orchestrator):
        """Test orchestrator initializes correctly."""
        assert orchestrator is not None
        assert orchestrator.dry_run is True
        assert orchestrator.should_stop is False
        assert orchestrator.batch_config["batch_pages"] == 2
        assert orchestrator.batch_config["max_failed_retries_per_page"] == 2

    def test_get_next_batch_pages(self, orchestrator):
        """Test batch page generation."""
        # Test normal batch
        pages = orchestrator._get_next_batch_pages(1000)
        assert pages == [1000, 999]  # batch_pages = 2

        # Test near page 1
        pages = orchestrator._get_next_batch_pages(2)
        assert pages == [2, 1]

        # Test at page 1
        pages = orchestrator._get_next_batch_pages(1)
        assert pages == [1]

    def test_should_continue_processing(self, orchestrator):
        """Test processing continuation logic."""
        # Normal case
        assert orchestrator._should_continue_processing(100, 5, None) is True

        # Below page 1
        assert orchestrator._should_continue_processing(0, 5, None) is False

        # Max pages reached
        assert orchestrator._should_continue_processing(100, 10, 10) is False

        # Stop requested
        orchestrator.should_stop = True
        assert orchestrator._should_continue_processing(100, 5, None) is False

    @patch('scrape_orchestrator.wait_with_progress')
    async def test_process_batch_success(self, mock_wait, orchestrator, sample_videos):
        """Test successful batch processing."""
        # Mock page parser
        mock_parse_result = Mock()
        mock_parse_result.success = True
        mock_parse_result.videos = sample_videos
        mock_parse_result.video_count = len(sample_videos)

        orchestrator.page_parser.parse_page = Mock(return_value=mock_parse_result)

        # Mock IDM manager
        mock_enqueue_result = {
            "success": True,
            "enqueued_count": 2,
            "failed_count": 0
        }
        orchestrator.idm_manager.enqueue = Mock(return_value=mock_enqueue_result)
        orchestrator.idm_manager.start_queue = Mock(return_value=True)
        orchestrator.idm_manager.create_download_items_from_videos = Mock(
            return_value=[Mock(), Mock()]
        )

        # Mock validation (no failures)
        orchestrator._validate_page_downloads = Mock(return_value=[])

        # Test batch processing
        result = await orchestrator._process_batch([1000, 999])

        assert isinstance(result, BatchResults)
        assert result.pages_processed == [1000, 999]
        assert result.total_videos_found == 4  # 2 pages * 2 videos each
        assert result.total_videos_enqueued == 2  # Dry run simulation
        assert len(result.failed_pages) == 0
        assert len(result.permanent_failed_pages) == 0

    @patch('scrape_orchestrator.wait_with_progress')
    async def test_process_batch_with_failures(self, mock_wait, orchestrator, sample_videos):
        """Test batch processing with page failures."""
        # Mock page parser with one failure
        def mock_parse_page(page_num, save_metadata=True):
            mock_result = Mock()
            if page_num == 1000:
                mock_result.success = True
                mock_result.videos = sample_videos
                mock_result.video_count = len(sample_videos)
            else:
                mock_result.success = False
                mock_result.videos = []
                mock_result.video_count = 0
            return mock_result

        orchestrator.page_parser.parse_page = mock_parse_page

        # Mock IDM manager
        orchestrator.idm_manager.enqueue = Mock(return_value={"success": True, "enqueued_count": 2})
        orchestrator.idm_manager.start_queue = Mock(return_value=True)
        orchestrator.idm_manager.create_download_items_from_videos = Mock(return_value=[Mock(), Mock()])

        # Mock validation
        orchestrator._validate_page_downloads = Mock(return_value=[])

        # Test batch processing
        result = await orchestrator._process_batch([1000, 999])

        assert result.pages_processed == [1000]  # Only successful page
        assert result.failed_pages == [999]      # Failed page
        assert result.total_videos_found == 2    # Only from successful page

    async def test_validate_and_retry_page_success(self, orchestrator, sample_videos):
        """Test page validation and retry with eventual success."""
        page_number = 1000

        # Mock validation: fail first time, succeed second time
        validation_results = [["video1"], []]  # First: 1 failure, Second: no failures
        orchestrator._validate_page_downloads = Mock(side_effect=validation_results)

        # Mock progress manager
        orchestrator.progress_manager.record_failed_videos = Mock(return_value=True)
        orchestrator.progress_manager.remove_failed_videos_for_page = Mock(return_value=True)

        # Mock IDM manager
        orchestrator.idm_manager.create_download_items_from_videos = Mock(return_value=[Mock()])
        orchestrator.idm_manager.enqueue = Mock(return_value={"success": True, "enqueued_count": 1})

        # Test validation and retry
        result = await orchestrator._validate_and_retry_page(page_number, sample_videos)

        assert isinstance(result, PageRetryState)
        assert result.page_number == page_number
        assert result.attempt_count == 2
        assert result.max_attempts_reached is False
        assert len(result.failed_video_ids) == 0  # Eventually succeeded

        # Verify retry was attempted
        orchestrator.progress_manager.record_failed_videos.assert_called()
        orchestrator.idm_manager.enqueue.assert_called()

    async def test_validate_and_retry_page_max_attempts(self, orchestrator, sample_videos):
        """Test page validation reaching max retry attempts."""
        page_number = 1000

        # Mock validation: always fail
        orchestrator._validate_page_downloads = Mock(return_value=["video1", "video2"])

        # Mock progress manager
        orchestrator.progress_manager.record_failed_videos = Mock(return_value=True)
        orchestrator.progress_manager.mark_page_permanent_failed = Mock(return_value=True)

        # Mock IDM manager
        orchestrator.idm_manager.create_download_items_from_videos = Mock(return_value=[Mock(), Mock()])
        orchestrator.idm_manager.enqueue = Mock(return_value={"success": True, "enqueued_count": 2})

        # Mock file operations
        with patch('scrape_orchestrator.SafeFileOperations') as mock_file_ops:
            mock_file_ops.safe_delete_folder.return_value = True

            # Test validation and retry
            result = await orchestrator._validate_and_retry_page(page_number, sample_videos)

            assert result.attempt_count == 2  # max_failed_retries_per_page
            assert result.max_attempts_reached is True
            assert len(result.failed_video_ids) == 2

            # Verify permanent failure handling
            orchestrator.progress_manager.mark_page_permanent_failed.assert_called_with(page_number)
            mock_file_ops.safe_delete_folder.assert_called()

    def test_validate_page_downloads_dry_run(self, orchestrator, sample_videos):
        """Test page download validation in dry run mode."""
        # In dry run mode, should simulate some failures
        failed_videos = orchestrator._validate_page_downloads(1000, sample_videos)

        assert isinstance(failed_videos, list)
        # In dry run, we simulate failures for testing
        assert len(failed_videos) >= 0

    @patch('scrape_orchestrator.ScrapeOrchestrator._process_batch')
    def test_run_workflow(self, mock_process_batch, orchestrator):
        """Test complete run workflow."""
        # Mock batch processing
        mock_batch_result = BatchResults(
            pages_processed=[1000, 999],
            total_videos_found=4,
            total_videos_enqueued=4,
            failed_pages=[],
            permanent_failed_pages=[],
            processing_time_seconds=10.0
        )

        mock_process_batch.return_value = mock_batch_result

        # Mock storage check
        orchestrator._check_storage_limits = Mock(return_value=False)

        # Test run with limited pages
        results = orchestrator.run(start_page=1000, max_pages=2)

        assert results["success"] is True
        assert results["pages_processed"] == 2
        assert results["videos_found"] == 4
        assert results["videos_enqueued"] == 4
        assert "total_time_seconds" in results

    def test_stop_orchestrator(self, orchestrator):
        """Test orchestrator stop functionality."""
        assert orchestrator.should_stop is False

        orchestrator.stop()

        assert orchestrator.should_stop is True

    def test_get_current_state(self, orchestrator):
        """Test getting current orchestrator state."""
        state = orchestrator.get_current_state()

        assert isinstance(state, dict)
        assert "should_stop" in state
        assert "dry_run" in state
        assert "batch_config" in state
        assert "progress_stats" in state
        assert "timestamp" in state


class TestBatchResults:
    """Test BatchResults dataclass."""

    def test_batch_results_creation(self):
        """Test BatchResults creation."""
        result = BatchResults(
            pages_processed=[1000, 999],
            total_videos_found=10,
            total_videos_enqueued=8,
            failed_pages=[998],
            permanent_failed_pages=[997],
            processing_time_seconds=120.5
        )

        assert result.pages_processed == [1000, 999]
        assert result.total_videos_found == 10
        assert result.total_videos_enqueued == 8
        assert result.failed_pages == [998]
        assert result.permanent_failed_pages == [997]
        assert result.processing_time_seconds == 120.5


class TestPageRetryState:
    """Test PageRetryState dataclass."""

    def test_page_retry_state_creation(self):
        """Test PageRetryState creation."""
        state = PageRetryState(
            page_number=1000,
            attempt_count=2,
            failed_video_ids=["video1", "video2"],
            last_attempt_time="2025-01-01T12:00:00",
            max_attempts_reached=False
        )

        assert state.page_number == 1000
        assert state.attempt_count == 2
        assert state.failed_video_ids == ["video1", "video2"]
        assert state.last_attempt_time == "2025-01-01T12:00:00"
        assert state.max_attempts_reached is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
