import { beforeEach, describe, expect, it, vi } from 'vitest';
import { openUrl } from '@tauri-apps/plugin-opener';
import { renderMarkdown } from './markdown';

vi.mock('@tauri-apps/plugin-opener', () => ({ openUrl: vi.fn(() => Promise.resolve()) }));

function render(text: string): HTMLElement {
  const root = document.createElement('div');
  renderMarkdown(root, text);
  return root;
}

function tags(root: HTMLElement, selector: string): string[] {
  return [...root.querySelectorAll(selector)].map((node) => node.textContent ?? '');
}

beforeEach(() => {
  vi.mocked(openUrl).mockClear();
});

describe('safety', () => {
  it('shows an img tag as literal text', () => {
    const root = render('<img src=x onerror=alert(1)>');
    expect(root.textContent).toBe('<img src=x onerror=alert(1)>');
    expect(root.querySelector('img')).toBeNull();
  });

  it('shows a script tag as literal text', () => {
    const root = render('<script>alert(1)</script>');
    expect(root.textContent).toBe('<script>alert(1)</script>');
    expect(root.querySelector('script')).toBeNull();
  });

  it('shows inline html as literal text', () => {
    const root = render('a <b onclick="x()">bold</b> c');
    expect(root.textContent).toBe('a <b onclick="x()">bold</b> c');
    expect(root.querySelector('b')).toBeNull();
  });

  it('makes no anchor for a javascript link', () => {
    expect(render('[x](javascript:alert(1))').querySelector('a')).toBeNull();
  });

  it('normalises an uppercase http scheme', () => {
    const anchor = render('[x](HTTP://Example.com)').querySelector('a.codex-link');
    expect(anchor?.getAttribute('href')).toBe('http://example.com/');
  });

  it('puts no event handler attributes on any element', () => {
    const root = render(
      [
        '<img src=x onerror=alert(1)>',
        '<a href="https://example.com" onclick="alert(1)">x</a>',
        '[x](javascript:alert(1)) [y](https://example.com "t\\" onmouseover=\\"alert(1)")',
        '<div onload=alert(1)>',
      ].join('\n\n'),
    );
    const attributes = [...root.querySelectorAll('*')].flatMap((node) => [...node.attributes].map((attr) => attr.name));
    expect(attributes.filter((name) => name.toLowerCase().startsWith('on'))).toEqual([]);
  });
});

describe('streaming', () => {
  it('renders an unclosed fence as code to the end', () => {
    const root = render('Here:\n\n```ts\nconst a = 1;\nconst b');
    expect(tags(root, 'pre.codex-code')).toEqual(['const a = 1;\nconst b']);
  });

  it('survives a reply cut off mid-construct', () => {
    for (const partial of ['[link](https://exa', '**bold', '- item\n  - ', '| a |\n|--', '> quote\n> ```']) {
      expect(() => render(partial)).not.toThrow();
    }
  });

  it('renders a large reply with dense markers quickly', () => {
    const chunk = '**a** _b_ *c* `d` [e](https://f.example) \\g <h> ~~i~~ src/**/*.j\n- k\n  1. l\n> m\n';
    const text = chunk.repeat(Math.ceil(20_000 / chunk.length));
    render(text);
    const started = performance.now();
    render(text);
    // The hand-written parser this replaced took about 60 ms on the same kind of input.
    expect(performance.now() - started).toBeLessThan(100);
  });
});
