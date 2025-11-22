import pytest
from unittest.mock import MagicMock
from dagster import build_op_context
from dagster_plus.resources.github_api import GithubAPI
from dagster_plus.assets.crawler.github import github_events, github_repos, github_repo_details

def test_github_events():
    mock_api = MagicMock(spec=GithubAPI)
    mock_api.get_events.return_value = [{"id": "1", "repo": {"id": 123, "name": "test/repo", "url": "https://api.github.com/repos/test/repo"}}]
    
    context = build_op_context()
    result = github_events(context=context, github_api=mock_api)
    
    assert len(result.value) == 1
    assert result.value[0]["id"] == "1"

def test_github_repos():
    events = [{"id": "1", "repo": {"id": 123, "name": "test/repo", "url": "https://api.github.com/repos/test/repo"}}]
    context = build_op_context()
    result = github_repos(context=context, github_events=events)
    
    assert len(result.value) == 1
    assert result.value[0]["id"] == 123

def test_github_repo_details():
    repos = [{"id": 123, "name": "test/repo", "url": "https://api.github.com/repos/test/repo"}]
    mock_api = MagicMock(spec=GithubAPI)
    mock_api.get_repo_details.return_value = {"id": 123, "name": "test/repo", "stargazers_count": 10}
    
    context = build_op_context()
    result = github_repo_details(context=context, github_repos=repos, github_api=mock_api)
    
    assert len(result.value) == 1
    assert result.value[0]["stargazers_count"] == 10
