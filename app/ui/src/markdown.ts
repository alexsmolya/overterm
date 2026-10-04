import { openUrl } from '@tauri-apps/plugin-opener';

function el<K extends keyof HTMLElementTagNameMap>(tag: K, className?: string, text?: string) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function safeMarkdownUrl(destination: string): string | null {
  if (!/^https?:\/\//i.test(destination)) return null;
  try {
    return new URL(destination).href;
  } catch {
    return null;
  }
}

function appendInlineMarkdown(target: HTMLElement, text: string) {
  let rest = text;
  while (rest) {
    if (rest.startsWith('\\') && rest.length > 1) {
      target.appendChild(document.createTextNode(rest[1]));
      rest = rest.slice(2);
      continue;
    }

    const code = rest.match(/^(`+)([\s\S]*?)\1/);
    if (code) {
      target.appendChild(el('code', 'codex-inline-code', code[2].replace(/\n/g, ' ')));
      rest = rest.slice(code[0].length);
      continue;
    }

    const link = rest.match(/^\[([^\]]+)\]\((https?:\/\/[^)\s]+)(?:\s+["'][^)]*["'])?\)/i);
    if (link) {
      const url = safeMarkdownUrl(link[2]);
      if (url) {
        const anchor = el('a', 'codex-link');
        appendInlineMarkdown(anchor, link[1]);
        anchor.href = url;
        anchor.addEventListener('click', (event) => {
          event.preventDefault();
          void openUrl(url).catch((error) => console.error(`could not open ${url}:`, error));
        });
        target.appendChild(anchor);
      } else {
        target.appendChild(document.createTextNode(link[0]));
      }
      rest = rest.slice(link[0].length);
      continue;
    }

    const strong = rest.match(/^(\*\*|__)(?=\S)([\s\S]*?\S)\1/);
    if (strong) {
      const node = el('strong');
      appendInlineMarkdown(node, strong[2]);
      target.appendChild(node);
      rest = rest.slice(strong[0].length);
      continue;
    }

    const emphasis = rest.match(/^(\*|_)(?=\S)([\s\S]*?\S)\1/);
    if (emphasis) {
      const node = el('em');
      appendInlineMarkdown(node, emphasis[2]);
      target.appendChild(node);
      rest = rest.slice(emphasis[0].length);
      continue;
    }

    const nextSpecial = rest.search(/[\\`[*_]/);
    if (nextSpecial < 0) {
      target.appendChild(document.createTextNode(rest));
      break;
    }
    if (nextSpecial > 0) {
      target.appendChild(document.createTextNode(rest.slice(0, nextSpecial)));
      rest = rest.slice(nextSpecial);
      continue;
    }

    target.appendChild(document.createTextNode(rest[0]));
    rest = rest.slice(1);
  }
}

function appendMarkdownParagraph(target: HTMLElement, lines: string[]) {
  const paragraph = el('div', 'codex-text');
  lines.forEach((line, index) => {
    if (index) paragraph.appendChild(document.createElement('br'));
    appendInlineMarkdown(paragraph, line);
  });
  target.appendChild(paragraph);
}

function appendMarkdownList(target: HTMLElement, lines: string[], ordered: boolean) {
  const list = el(ordered ? 'ol' : 'ul', `codex-list ${ordered ? 'ordered' : 'unordered'}`);
  for (const line of lines) {
    const match = ordered ? line.match(/^ {0,3}\d+[.)]\s+(.*)$/) : line.match(/^ {0,3}[-+*]\s+(.*)$/);
    if (!match) break;
    const item = el('li');
    appendInlineMarkdown(item, match[1]);
    list.appendChild(item);
  }
  target.appendChild(list);
}

/// Agent replies are Markdown. This is deliberately a small renderer rather
/// than an HTML parser: every node is created by hand and all Codex text enters
/// through text nodes, so even HTML-looking model output remains inert text.
export function renderMarkdown(target: HTMLElement, text: string) {
  target.replaceChildren();
  const lines = text.replace(/\r\n?/g, '\n').split('\n');
  let paragraph: string[] = [];
  const flushParagraph = () => {
    if (paragraph.length) appendMarkdownParagraph(target, paragraph);
    paragraph = [];
  };

  for (let index = 0; index < lines.length; ) {
    const line = lines[index];
    const fence = line.match(/^ {0,3}```[^\n]*$/);
    if (fence) {
      flushParagraph();
      const code: string[] = [];
      index += 1;
      while (index < lines.length && !/^ {0,3}```\s*$/.test(lines[index])) {
        code.push(lines[index]);
        index += 1;
      }
      if (index < lines.length) index += 1;
      target.appendChild(el('pre', 'codex-code', code.join('\n')));
      continue;
    }

    const heading = line.match(/^ {0,3}(#{1,6})\s+(.*?)\s*#*\s*$/);
    if (heading) {
      flushParagraph();
      const title = el(`h${heading[1].length}` as keyof HTMLElementTagNameMap, 'codex-heading');
      appendInlineMarkdown(title, heading[2]);
      target.appendChild(title);
      index += 1;
      continue;
    }

    if (/^ {0,3}[-+*]\s+/.test(line)) {
      flushParagraph();
      const items: string[] = [];
      while (index < lines.length && /^ {0,3}[-+*]\s+/.test(lines[index])) items.push(lines[index++]);
      appendMarkdownList(target, items, false);
      continue;
    }

    if (/^ {0,3}\d+[.)]\s+/.test(line)) {
      flushParagraph();
      const items: string[] = [];
      while (index < lines.length && /^ {0,3}\d+[.)]\s+/.test(lines[index])) items.push(lines[index++]);
      appendMarkdownList(target, items, true);
      continue;
    }

    if (!line.trim()) {
      flushParagraph();
      index += 1;
      continue;
    }

    paragraph.push(line);
    index += 1;
  }
  flushParagraph();
}
