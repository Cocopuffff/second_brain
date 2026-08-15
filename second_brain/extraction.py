from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Protocol


class ExtractionError(RuntimeError):
    pass


class ImageProcessor(Protocol):
    def describe(self, *, src: str, alt: str, caption: str, context: str) -> str | None: ...


class CaptionImageProcessor:
    """Safe default: only retains evidence already supplied by the document."""

    def describe(self, *, src: str, alt: str, caption: str, context: str) -> str | None:
        useful = caption.strip() or alt.strip()
        return useful if useful else None


@dataclass
class Node:
    tag: str
    attrs: dict[str, str]
    children: list["Node | str"] = field(default_factory=list)


class _TreeParser(HTMLParser):
    VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = Node("root", {})
        self.stack = [self.root]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        node = Node(tag.lower(), {k.lower(): v or "" for k, v in attrs})
        self.stack[-1].children.append(node)
        if tag.lower() not in self.VOID:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if self.stack[-1].tag == tag.lower():
            self.stack.pop()

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                return

    def handle_data(self, data: str) -> None:
        self.stack[-1].children.append(data)


REMOVED_TAGS = {"script", "style", "nav", "footer", "form", "noscript", "svg", "canvas", "template", "aside"}
CHROME_WORDS = re.compile(r"(?:advert|sponsor|cookie|consent|newsletter|subscribe|related|share|social|breadcrumb|sidebar|promo|recommended)", re.I)
BLOCKS = {"p", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre", "ul", "ol", "table", "figure", "img", "hr", "div", "section", "article"}


def _text(node: Node | str) -> str:
    if isinstance(node, str):
        return node
    return "".join(_text(child) for child in node.children)


def _is_removed(node: Node) -> bool:
    marker = f"{node.attrs.get('id', '')} {node.attrs.get('class', '')}"
    return node.tag in REMOVED_TAGS or bool(CHROME_WORDS.search(marker))


def _first(root: Node, tag: str) -> Node | None:
    if root.tag == tag:
        return root
    for child in root.children:
        if isinstance(child, Node):
            found = _first(child, tag)
            if found:
                return found
    return None


def _caption(node: Node) -> str:
    for child in node.children:
        if isinstance(child, Node) and child.tag in {"figcaption", "caption"}:
            return " ".join(_text(child).split())
    return ""


def _contains(node: Node, tag: str) -> bool:
    return node.tag == tag or any(isinstance(child, Node) and _contains(child, tag) for child in node.children)


def extract_article(html_text: str, image_processor: ImageProcessor | None = None) -> dict[str, str]:
    parser = _TreeParser()
    parser.feed(html_text)
    root = parser.root
    title_node = _first(root, "title")
    title = " ".join(_text(title_node).split()) if title_node else "Untitled article"
    body = _first(root, "article") or _first(root, "main") or _first(root, "body") or root
    processor = image_processor or CaptionImageProcessor()
    blocks: list[str] = []

    def visit(node: Node, inherited_caption: str = "") -> None:
        if _is_removed(node):
            return
        caption = _caption(node) or inherited_caption
        if node.tag == "img":
            alt = node.attrs.get("alt", "")
            context = " ".join(_text(body).split())[:500]
            description = processor.describe(src=node.attrs.get("src", ""), alt=alt, caption=caption, context=context)
            if description is None:
                if alt.strip() or caption.strip():
                    return
                raise ExtractionError("meaningful image could not be inspected reliably")
            image_block = "```image\n" + " ".join(description.split()) + "\n"
            if caption.strip():
                image_block += "Caption: " + " ".join(caption.split()) + "\n"
            image_block += "```"
            blocks.append(image_block)
            return
        if node.tag in {"p", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre"}:
            if _contains(node, "img"):
                for child in node.children:
                    if isinstance(child, Node):
                        visit(child, caption)
                text_value = " ".join(value for value in (_text(child).strip() for child in node.children if isinstance(child, str)) if value)
                if text_value:
                    blocks.append(text_value)
                return
            content = " ".join(_text(node).split())
            if content:
                if node.tag.startswith("h"):
                    blocks.append(f"{'#' * int(node.tag[1])} {content}")
                elif node.tag == "blockquote":
                    blocks.append("\n".join(f"> {line}" for line in content.splitlines() if line.strip()))
                elif node.tag == "pre":
                    blocks.append("```\n" + _text(node).strip("\n") + "\n```")
                else:
                    blocks.append(content)
            return
        if node.tag in {"ul", "ol"}:
            items = []
            for child in node.children:
                if isinstance(child, Node) and child.tag == "li":
                    text_value = " ".join(_text(child).split())
                    if text_value:
                        items.append(text_value)
            prefix = "1." if node.tag == "ol" else "-"
            if items:
                blocks.append("\n".join(f"{prefix} {item}" for item in items))
            return
        if node.tag == "table":
            rows: list[list[str]] = []
            for tr in _descendants(node, "tr"):
                cells = [_text(cell).strip().replace("|", "\\|") for cell in tr.children if isinstance(cell, Node) and cell.tag in {"th", "td"}]
                if cells:
                    rows.append(cells)
            if rows:
                width = max(map(len, rows))
                rows = [row + [""] * (width - len(row)) for row in rows]
                blocks.append("| " + " | ".join(rows[0]) + " |\n| " + " | ".join("---" for _ in range(width)) + " |\n" + "\n".join("| " + " | ".join(row) + " |" for row in rows[1:]))
            return
        for child in node.children:
            if isinstance(child, Node):
                visit(child, caption)

    visit(body)
    content = "\n\n".join(block for block in blocks if block.strip())
    content = re.sub(r"\n{3,}", "\n\n", content).strip()
    return {"title": title, "body": content}


def _descendants(node: Node, tag: str):
    for child in node.children:
        if isinstance(child, Node):
            if child.tag == tag:
                yield child
            yield from _descendants(child, tag)
