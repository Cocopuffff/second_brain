from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


TRACKING_PARAMETERS = {
    "_hsenc",
    "_hsmi",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}
YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"}


class InvalidLocator(ValueError):
    pass


def youtube_video_id(value: str) -> str | None:
    parts = urlsplit(value)
    host = (parts.hostname or "").lower()
    if host not in YOUTUBE_HOSTS:
        return None
    if host == "youtu.be":
        candidate = parts.path.strip("/").split("/")[0]
    elif parts.path == "/watch":
        candidate = dict(parse_qsl(parts.query, keep_blank_values=True)).get("v", "")
    else:
        match = re.fullmatch(r"/(?:shorts|embed|live)/([^/?#]+)", parts.path)
        candidate = match.group(1) if match else ""
    return candidate if re.fullmatch(r"[A-Za-z0-9_-]{6,20}", candidate) else None


def canonical_url(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidLocator("URL is empty")
    parts = urlsplit(value.strip())
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"} or not parts.hostname:
        raise InvalidLocator("only absolute HTTP(S) URLs are supported")
    video_id = youtube_video_id(value)
    if video_id:
        return f"https://www.youtube.com/watch?v={video_id}"
    try:
        hostname = parts.hostname.encode("idna").decode("ascii").lower()
        port = parts.port
    except (UnicodeError, ValueError) as exc:
        raise InvalidLocator("invalid hostname or port") from exc
    if (scheme, port) in {("http", 80), ("https", 443)}:
        port = None
    netloc = hostname
    if parts.username or parts.password:
        raise InvalidLocator("credentials in URLs are not supported")
    if ":" in hostname and not hostname.startswith("["):
        netloc = f"[{hostname}]"
    if port:
        netloc = f"{netloc}:{port}"
    path = re.sub(r"/+/", "/", parts.path or "/")
    segments = []
    for segment in path.split("/"):
        if segment in {"", "."}:
            continue
        if segment == "..":
            if segments:
                segments.pop()
        else:
            segments.append(segment)
    path = "/" + "/".join(segments)
    if parts.path.endswith("/") and path != "/":
        path += "/"
    query = [(key, val) for key, val in parse_qsl(parts.query, keep_blank_values=True) if key.lower() not in TRACKING_PARAMETERS]
    return urlunsplit((scheme, netloc, path, urlencode(query, doseq=True), ""))


def source_key(kind: str, locator: str) -> str:
    canonical = canonical_url(locator) if kind == "article" else canonical_url(locator)
    return f"{kind}:{canonical}"


def stable_id(kind: str, key: str) -> str:
    return f"{kind}-{hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]}"
