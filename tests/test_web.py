"""Prevent stale console assets when switching database versions."""

import re
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

import kvdb.demo.__main__ as web


def test_web_asset_versions_and_cache_control(tmp_path, monkeypatch):
    assets = tmp_path / "static"
    assets.mkdir()
    (assets / "dashboard.js").write_text("window.version = 1;", encoding="utf-8")
    (assets / "style.css").write_text("body { color: white; }", encoding="utf-8")
    (tmp_path / "index.html").write_text(
        '<link href="/assets/style.css"><script src="/assets/dashboard.js"></script>',
        encoding="utf-8",
    )
    monkeypatch.setattr(web, "__file__", str(tmp_path / "__main__.py"))
    demo = MagicMock()
    demo.state.return_value = {"initialized": True}
    monkeypatch.setattr(web, "Demo", MagicMock(return_value=demo))

    def load_assets():
        app = web.create_demo_app({"leader": "http://leader"})
        with TestClient(app) as client:
            page = client.get("/")
            assert page.status_code == 200
            assert page.headers["cache-control"] == "no-store"
            urls = re.findall(r'/assets/[^"<>]+', page.text)
            assert len(urls) == 2 and all("?v=" in url for url in urls)
            for url in urls:
                response = client.get(url)
                assert response.status_code == 200
                assert response.headers["cache-control"] == "no-store"
            assert client.get("/api/state").headers["cache-control"] == "no-store"
            script = next(url for url in urls if "dashboard.js" in url)
            return urls, client.get(script).text

    original_urls, original_script = load_assets()
    (assets / "dashboard.js").write_text("window.version = 2;", encoding="utf-8")
    updated_urls, updated_script = load_assets()
    assert original_urls != updated_urls
    assert "version = 1" in original_script
    assert "version = 2" in updated_script
