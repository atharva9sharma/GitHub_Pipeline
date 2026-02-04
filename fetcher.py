import requests
import os
import time

GITHUB_API = "https://api.github.com"

class DataFetcher:
    def __init__(self, token=None):
        self.token = token or os.getenv("GITHUB_TOKEN")
        if not self.token:
            print("Warning: No GITHUB_TOKEN found. API rate limits will be restricted.")
            self.headers = {}
        else:
            self.headers = {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github.v3+json"
            }

    def _get(self, url, params=None):
        """Helper to make GET requests with error handling."""
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None

    def fetch_items(self, repo, item_type, since=None, until=None, limit=None):
        """Generic fetcher for issues or specific types."""
        url = f"{GITHUB_API}/repos/{repo}/{item_type}"
        
        all_items = []
        page = 1
        per_page = 100
        
        params = {
            "state": "all",
            "per_page": per_page,
            "page": page,
            "sort": "created",
            "direction": "asc"
        }
        
        if since:
            params['since'] = since
        
        while True:
            params['page'] = page
            items = self._get(url, params)
            
            if not items:
                break
                
            for item in items:
                created_at = item.get('created_at')
                # Client-side filtering for 'until'
                if until and created_at > until:
                     continue
                
                all_items.append(item)
                if limit and len(all_items) >= limit:
                    return all_items

            if len(items) < per_page:
                break
                
            page += 1
            
        return all_items

    def fetch_commits(self, repo, since=None, until=None):
        """Fetch commits with date filtering."""
        url = f"{GITHUB_API}/repos/{repo}/commits"
        params = {
            "per_page": 100,
            "page": 1,
        }
        if since: params['since'] = since
        if until: params['until'] = until

        all_commits = []
        while True:
            commits = self._get(url, params)
            if not commits:
                break
            
            all_commits.extend(commits)
            if len(commits) < 100:
                break
            params['page'] += 1
            
        return all_commits

