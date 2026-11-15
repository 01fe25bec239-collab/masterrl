import { useEffect, useId, useMemo, useState } from 'react';
import { cn } from '../lib/cn';
import { Dialog } from './Dialog';
import { Icon, type IconName } from './Icon';

export interface Command {
  id: string;
  label: string;
  /** Heading the command is listed under, e.g. "Go to". */
  group?: string;
  /** Extra words that should find it. */
  keywords?: string[];
  icon?: IconName;
  run: () => void;
}

export interface CommandPaletteProps {
  commands: Command[];
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

function matches(command: Command, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const hay = [command.label, command.group ?? '', ...(command.keywords ?? [])]
    .join(' ')
    .toLowerCase();
  return q.split(/\s+/).every((word) => hay.includes(word));
}

/** ⌘K / Ctrl+K search over commands. */
export function CommandPalette({ commands, open, onOpenChange }: CommandPaletteProps) {
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const listId = useId();
  const results = useMemo(() => commands.filter((c) => matches(c, query)), [commands, query]);
  const current = Math.min(active, Math.max(results.length - 1, 0));

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        onOpenChange(!open);
      }
    };
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('keydown', onKey);
    };
  }, [open, onOpenChange]);

  const close = () => {
    setQuery('');
    setActive(0);
    onOpenChange(false);
  };
  const run = (command: Command | undefined) => {
    if (!command) return;
    close();
    command.run();
  };

  return (
    <Dialog open={open} onClose={close} title="Search commands" hideTitle className="mt-[15vh]">
      <div className="flex items-center gap-2 rounded-md border border-border px-3">
        <Icon name="search" className="text-fg-muted" />
        <input
          // eslint-disable-next-line jsx-a11y/no-autofocus -- the palette exists to type into
          autoFocus
          type="text"
          role="combobox"
          aria-expanded="true"
          aria-controls={listId}
          aria-activedescendant={results[current] ? `${listId}-${results[current].id}` : undefined}
          aria-label="Search commands"
          placeholder="Type a command or page name"
          className="w-full bg-transparent py-2 text-fg outline-none"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setActive(0);
          }}
          onKeyDown={(e) => {
            if (e.key === 'ArrowDown') {
              e.preventDefault();
              setActive(Math.min(current + 1, results.length - 1));
            } else if (e.key === 'ArrowUp') {
              e.preventDefault();
              setActive(Math.max(current - 1, 0));
            } else if (e.key === 'Enter') {
              e.preventDefault();
              run(results[current]);
            }
          }}
        />
      </div>
      <div id={listId} role="listbox" aria-label="Commands" className="mt-3 max-h-80 overflow-auto">
        {results.map((command, i) => (
          // eslint-disable-next-line jsx-a11y/click-events-have-key-events -- keys go to the combobox input via aria-activedescendant
          <div
            key={command.id}
            id={`${listId}-${command.id}`}
            role="option"
            tabIndex={-1}
            aria-selected={i === current}
            className={cn(
              'flex cursor-pointer items-center gap-2 rounded-md px-3 py-2',
              i === current ? 'bg-accent text-accent-fg' : 'text-fg',
            )}
            onMouseEnter={() => {
              setActive(i);
            }}
            onClick={() => {
              run(command);
            }}
          >
            {command.icon && <Icon name={command.icon} />}
            <span>{command.label}</span>
            {command.group && (
              <span className={cn('ml-auto text-xs', i === current ? '' : 'text-fg-muted')}>
                {command.group}
              </span>
            )}
          </div>
        ))}
      </div>
      {results.length === 0 && (
        <p className="px-3 py-2 text-fg-muted">Nothing matches “{query}”. Try a shorter word.</p>
      )}
    </Dialog>
  );
}
